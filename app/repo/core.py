"""Mirror repository core: durable, idempotent prepare/activate with receipts.

Idempotency contract (keyed by the op key derived from the release id):
  - same key + same digest  -> replay the FIRST stored receipt, no state change
  - same key + other digest -> explicit rejection (Conflict)
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading

from app.common.receipts import new_receipt, utcnow

SCHEMA = """
CREATE TABLE IF NOT EXISTS ops (
  op_key       TEXT PRIMARY KEY,
  op           TEXT NOT NULL,
  digest       TEXT NOT NULL,
  receipt      TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  prev_digest  TEXT,
  rolled_back  INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS artifacts (
  digest TEXT PRIMARY KEY,
  data   BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
"""


class Conflict(Exception):
    def __init__(self, existing_digest: str):
        self.existing_digest = existing_digest
        super().__init__(f"op key already bound to digest {existing_digest}")


class NotPrepared(Exception):
    pass


class DisconnectedAfterCommit(Exception):
    """The op was committed, but the repo now simulates a dropped response."""


class RepoCore:
    def __init__(self, db_path: str, name: str, secret: str):
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.name = name
        self.secret = secret
        self._lock = threading.RLock()
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=FULL")
            self._db.executescript(SCHEMA)
            # Forward migration for volumes created before rollback support.
            cols = {r[1] for r in self._db.execute("PRAGMA table_info(ops)")}
            if "prev_digest" not in cols:
                self._db.execute("ALTER TABLE ops ADD COLUMN prev_digest TEXT")
            if "rolled_back" not in cols:
                self._db.execute("ALTER TABLE ops ADD COLUMN rolled_back INTEGER NOT NULL DEFAULT 0")
            self._db.commit()
        # Fault-injection state (volatile, test-only).
        self.disconnected = False
        self._arm_disconnect_after_activate = False
        self._arm_corrupt_next_activate = False

    def close(self) -> None:
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None

    # ---- fault hooks (test-only) ----
    def set_disconnected(self, flag: bool) -> None:
        with self._lock:
            self.disconnected = flag

    def arm_disconnect_after_activate(self) -> None:
        with self._lock:
            self._arm_disconnect_after_activate = True

    def arm_corrupt_next_activate(self) -> None:
        with self._lock:
            self._arm_corrupt_next_activate = True

    def fault_state(self) -> dict:
        with self._lock:
            return {
                "disconnected": self.disconnected,
                "disconnect_after_activate": self._arm_disconnect_after_activate,
                "corrupt_next_activate": self._arm_corrupt_next_activate,
            }

    # ---- operations ----
    def prepare(self, op_key: str, digest: str, artifact: bytes):
        """Stage an artifact. Returns (receipt, created)."""
        if hashlib.sha256(artifact).hexdigest() != digest:
            raise ValueError("artifact digest does not match the declared digest")
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM ops WHERE op_key=?", (op_key,)
            ).fetchone()
            if row:
                if row["op"] == "prepare" and row["digest"] == digest:
                    return json.loads(row["receipt"]), False  # replay first receipt
                raise Conflict(row["digest"])
            receipt = new_receipt(self.name, "prepare", op_key, digest, self.secret)
            self._db.execute(
                "INSERT INTO ops(op_key, op, digest, receipt, created_at)"
                " VALUES(?,?,?,?,?)",
                (op_key, "prepare", digest, json.dumps(receipt), utcnow()),
            )
            self._db.execute(
                "INSERT OR IGNORE INTO artifacts(digest, data) VALUES(?,?)",
                (digest, artifact),
            )
            self._kv_inc("prepare_count")
            self._db.commit()
            return receipt, True

    def activate(self, op_key: str, digest: str):
        """Flip the active pointer to a staged digest. Returns (receipt, created)."""
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM ops WHERE op_key=?", (op_key,)
            ).fetchone()
            if row:
                if row["op"] == "activate" and row["digest"] == digest:
                    return json.loads(row["receipt"]), False  # replay first receipt
                raise Conflict(row["digest"])
            staged = self._db.execute(
                "SELECT 1 FROM artifacts WHERE digest=?", (digest,)
            ).fetchone()
            if not staged:
                raise NotPrepared(digest)
            if self._arm_corrupt_next_activate:
                self._arm_corrupt_next_activate = False
                # Misbehaving repo: returns a well-formed, properly signed receipt
                # carrying a digest that does NOT belong to the release. Crucially
                # no state changes: the active pointer is NOT rewritten and nothing
                # is persisted under this op key.
                bogus = hashlib.sha256(f"corrupt|{op_key}".encode()).hexdigest()
                return new_receipt(self.name, "activate", op_key, bogus, self.secret), True
            receipt = new_receipt(self.name, "activate", op_key, digest, self.secret)
            prev_digest = self._kv_get("active_digest")
            self._db.execute(
                "INSERT INTO ops(op_key, op, digest, receipt, created_at, prev_digest)"
                " VALUES(?,?,?,?,?,?)",
                (op_key, "activate", digest, json.dumps(receipt), utcnow(), prev_digest),
            )
            self._kv_set("active_digest", digest)
            self._kv_inc("activation_count")
            self._db.commit()
            if self._arm_disconnect_after_activate:
                self._arm_disconnect_after_activate = False
                self.disconnected = True
                raise DisconnectedAfterCommit()
            return receipt, True

    def get_op(self, op_key: str) -> dict | None:
        with self._lock:
            row = self._db.execute(
                "SELECT receipt FROM ops WHERE op_key=?", (op_key,)
            ).fetchone()
        return json.loads(row["receipt"]) if row else None

    def rollback_activate(self, op_key: str) -> bool:
        """Undo the activation committed under op_key.

        Restores the active pointer captured immediately BEFORE that
        activation (which may be None on a fresh volume). Idempotent: repeated
        calls keep the same prior pointer, and a rollback never clobbers a
        newer activation that superseded the undone digest. Returns True when
        an activate op key was found and handled, False when nothing under
        this key ever committed an activation (e.g. a forged fault receipt).
        """
        with self._lock:
            row = self._db.execute(
                "SELECT * FROM ops WHERE op_key=?", (op_key,)
            ).fetchone()
            if row is None or row["op"] != "activate":
                return False
            current = self._kv_get("active_digest")
            if not row["rolled_back"]:
                # The pointer is restored only while the undone activation is
                # the one that owns it; a newer release must never be reverted.
                if current == row["digest"]:
                    self._restore_pointer(row["prev_digest"])
                # The counter reflects committed, non-rolled-back activations.
                count = int(self._kv_get("activation_count") or 0)
                self._kv_set("activation_count", str(max(0, count - 1)))
                self._db.execute(
                    "UPDATE ops SET rolled_back=1 WHERE op_key=?", (op_key,)
                )
                self._db.commit()
                return True
            # Idempotent replay: re-assert the old pointer only while the
            # undone digest is still active (never overwrite a newer release).
            if current == row["digest"]:
                self._restore_pointer(row["prev_digest"])
                self._db.commit()
            return True

    def _restore_pointer(self, prev_digest: str | None) -> None:
        """Call with the lock held."""
        if prev_digest is None:
            self._db.execute("DELETE FROM kv WHERE k=?", ("active_digest",))
        else:
            self._kv_set("active_digest", prev_digest)

    def state(self) -> dict:
        with self._lock:
            active = self._kv_get("active_digest")
            activations = int(self._kv_get("activation_count") or 0)
            prepares = int(self._kv_get("prepare_count") or 0)
            staged = [
                r[0]
                for r in self._db.execute("SELECT digest FROM artifacts ORDER BY digest")
            ]
            disconnected = self.disconnected
        return {
            "repo": self.name,
            "active_digest": active,
            "activation_count": activations,
            "prepare_count": prepares,
            "staged": staged,
            "disconnected": disconnected,
        }

    # ---- kv helpers (call with the lock held) ----
    def _kv_get(self, k: str):
        row = self._db.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
        return row[0] if row else None

    def _kv_set(self, k: str, v: str) -> None:
        self._db.execute(
            "INSERT INTO kv(k, v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (k, v),
        )

    def _kv_inc(self, k: str) -> None:
        self._kv_set(k, str(int(self._kv_get(k) or 0) + 1))
