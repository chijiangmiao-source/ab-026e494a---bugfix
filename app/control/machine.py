"""Release state machine: drives prepare/activate across both mirror repos.

Crash-safety model: the release intent is persisted before any repo call, and
every repo receipt is persisted locally as soon as it is observed. Before
issuing an operation the machine first asks the repo for an existing receipt
under the derived op key, so a control-service restart converges from
repo-side receipts without ever re-executing an operation.
"""
from __future__ import annotations

import base64
import logging
import threading

from app.common.httpjson import TransportError
from app.common.receipts import verify_receipt
from app.control import core

log = logging.getLogger("control.machine")


class Rejected(Exception):
    """Internal signal: evidence conflicts with the release intent.

    Carrying this signal does NOT lock the release immediately: the machine
    first enters REJECTING and rolls back every repo that may have flipped its
    active pointer, so a rejected candidate can never remain the active
    version on any mirror. The terminal REJECTED lock is set only after
    rollback has converged (or been confirmed unnecessary) on every repo.
    """


# Suffix the repo uses to derive a rollback op key from its activate op key
# (must match RepoCore._rollback_key).
ROLLBACK_SUFFIX = ":rollback"


class ReleaseMachine:
    def __init__(self, store, clients: dict, secrets: dict):
        self.store = store
        self.clients = clients  # repo name -> RepoClient
        self.secrets = secrets  # repo name -> HMAC secret

    def advance(self, release_id: str) -> None:
        rel = self.store.get_release(release_id)
        if rel is None or rel["state"] in core.TERMINAL_STATES:
            return
        try:
            if rel["state"] == core.STATE_REJECTING:
                self._run_rejecting(rel)
            else:
                self._run(rel)
        except Rejected as decision:
            # A rejection was decided: park in REJECTING so the next tick can
            # converge rollbacks before the terminal lock is taken.
            self._begin_rejection(release_id, str(decision))
        except Exception:  # noqa: BLE001 - keep the worker alive
            log.exception("advance failed for release %s", release_id)

    # ---- forward progress: prepare -> activate ----
    def _run(self, rel: dict) -> None:
        rid, sha = rel["release_id"], rel["sha256"]
        artifact_b64 = base64.b64encode(rel["artifact"]).decode()

        # Phase 1: prepare both repos with the identical candidate bytes.
        for repo in self.clients:
            receipt = self._ensure_op(rid, repo, core.OP_PREPARE, sha, artifact_b64)
            if receipt is None:
                self._set_state(rid, core.STATE_PREPARING)
                return
            self._check_digest(rid, repo, core.OP_PREPARE, sha, receipt)

        self._set_state(rid, core.STATE_ACTIVATING)

        # Phase 2: activate both repos; only identical digests may complete.
        for repo in self.clients:
            receipt = self._ensure_op(rid, repo, core.OP_ACTIVATE, sha, artifact_b64)
            if receipt is None:
                return
            self._check_digest(rid, repo, core.OP_ACTIVATE, sha, receipt)

        self._set_state(rid, core.STATE_COMPLETED)
        log.info("release %s completed (sha256=%s)", rid, sha)

    # ---- rejection convergence: roll every repo back, then lock ----
    def _run_rejecting(self, rel: dict) -> None:
        rid = rel["release_id"]
        for repo in self.clients:
            if self._ensure_rollback(rid, repo) is None:
                return  # a repo is unreachable; keep converging on later ticks
        self._set_state(rid, core.STATE_REJECTED, error=rel["error"])
        log.info("release %s locked as REJECTED after rollback convergence", rid)

    def _ensure_rollback(self, rid: str, repo: str) -> dict | None:
        """Drive one repo back to its pre-release active pointer.

        Returns a settled evidence/marker dict, or None while the repo is
        unreachable (the worker retries without locking the release).
        """
        existing = self.store.get_receipt(rid, repo, core.OP_ROLLBACK)
        if existing is not None:
            return existing
        client = self.clients[repo]
        activate_key = core.op_key(rid, repo, core.OP_ACTIVATE)
        rollback_key = activate_key + ROLLBACK_SUFFIX
        try:
            # Post-crash adoption: the rollback receipt may already exist.
            status, body = client.get_op(rollback_key)
            if status == 200:
                return self._adopt(rid, repo, core.OP_ROLLBACK, rollback_key,
                                   body.get("receipt"))
            if status == 404:
                status, body = client.rollback(activate_key)
                if status in (200, 201) and body.get("rolled_back"):
                    return self._adopt(rid, repo, core.OP_ROLLBACK, rollback_key,
                                       body.get("receipt"))
                if status in (200, 201):
                    # Repo never activated under this key: pointer is already
                    # at its pre-release value; record the no-op as settled.
                    marker = {
                        "op": core.OP_ROLLBACK,
                        "op_key": rollback_key,
                        "rolled_back": False,
                        "digest": "",
                        "note": "no activation to roll back",
                    }
                    self.store.put_receipt(rid, repo, core.OP_ROLLBACK,
                                           rollback_key, "", marker)
                    return marker
                log.warning("repo %s rollback for %s -> HTTP %s", repo, rid, status)
                return None
            log.warning("repo %s rollback lookup for %s -> HTTP %s", repo, rid, status)
            return None
        except TransportError as e:
            log.warning("repo %s unreachable for rollback %s: %s", repo, rid, e)
            return None

    def _begin_rejection(self, rid: str, message: str) -> None:
        rel = self.store.get_release(rid)
        if rel is None or rel["state"] in core.TERMINAL_STATES:
            return
        if rel["state"] != core.STATE_REJECTING:
            log.error("release %s entering rejection: %s", rid, message)
            self.store.update_state(rid, core.STATE_REJECTING, message)

    def _ensure_op(self, rid: str, repo: str, op: str, sha: str,
                   artifact_b64: str) -> dict | None:
        """Return the repo receipt for this op, persisting it locally.

        Returns None when the repo is unreachable (the worker retries later).
        Raises Rejected when repo-side evidence conflicts with the release.
        """
        existing = self.store.get_receipt(rid, repo, op)
        if existing is not None:
            return existing
        client = self.clients[repo]
        key = core.op_key(rid, repo, op)
        try:
            # Post-crash adoption: the repo may already hold the first receipt.
            status, body = client.get_op(key)
            if status == 200:
                return self._adopt(rid, repo, op, key, body.get("receipt"))
            if status == 404:
                if op == core.OP_PREPARE:
                    status, body = client.prepare(key, sha, artifact_b64)
                else:
                    status, body = client.activate(key, sha)
                if status in (200, 201):
                    return self._adopt(rid, repo, op, key, body.get("receipt"))
                if status == 409:
                    self._reject_conflict(rid, repo, op, key, body)
                if status == 400 and (body.get("error") or {}).get("code") == "not_prepared":
                    # Repo lost its staging area; re-prepare, retry next tick.
                    client.prepare(core.op_key(rid, repo, core.OP_PREPARE), sha, artifact_b64)
                return None
            # 5xx or anything unexpected: treat as temporarily unreachable.
            log.warning("repo %s %s for %s -> HTTP %s", repo, op, rid, status)
            return None
        except TransportError as e:
            log.warning("repo %s unreachable for %s %s: %s", repo, op, rid, e)
            return None

    def _adopt(self, rid: str, repo: str, op: str, key: str, receipt) -> dict | None:
        if not isinstance(receipt, dict) or not receipt:
            log.warning("repo %s returned an empty receipt for %s", repo, key)
            return None
        if not verify_receipt(self.secrets.get(repo, ""), receipt):
            raise Rejected(
                f"镜像仓 {repo} 的 {op} 证据签名校验失败，发布将在回滚收敛后锁定为拒绝"
            )
        self.store.put_receipt(rid, repo, op, key, str(receipt.get("digest", "")), receipt)
        return receipt

    def _check_digest(self, rid: str, repo: str, op: str, sha: str, receipt: dict) -> None:
        if receipt.get("digest") != sha:
            raise Rejected(
                f"镜像仓 {repo} 的 {op} 回执摘要不属于本发布"
                f"（收到 {receipt.get('digest')}，期望 {sha}），"
                f"发布将在回滚收敛后锁定为拒绝"
            )

    def _reject_conflict(self, rid: str, repo: str, op: str, key: str, body: dict) -> None:
        err = body.get("error") or {}
        existing = err.get("existing_digest", "<unknown>")
        # Best effort: keep the conflicting repo-side receipt as evidence.
        try:
            status, body2 = self.clients[repo].get_op(key)
            if status == 200:
                receipt = body2.get("receipt")
                if isinstance(receipt, dict) and receipt:
                    self.store.put_receipt(
                        rid, repo, op, key, str(receipt.get("digest", "")), receipt
                    )
        except TransportError:
            pass
        raise Rejected(
            f"镜像仓 {repo} 拒绝了 {op}：操作键已绑定不同摘要（{existing}），"
            f"发布将在回滚收敛后锁定为拒绝"
        )

    def _set_state(self, rid: str, state: str, error: str | None = None) -> None:
        rel = self.store.get_release(rid)
        if rel is None or rel["state"] in core.TERMINAL_STATES:
            return  # terminal states are locked and never rewritten
        if rel["state"] != state or error:
            self.store.update_state(rid, state, error)


class Worker(threading.Thread):
    """Background reconciler: advances every non-terminal release.

    On process start it picks up all unfinished releases from the durable
    store, which is what makes a control-service restart converge.
    """

    def __init__(self, store, machine: ReleaseMachine, interval: float = 0.5):
        super().__init__(name="control-worker", daemon=True)
        self.store = store
        self.machine = machine
        self.interval = interval
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                for rid in self.store.pending_release_ids(core.TERMINAL_STATES):
                    if self._stop_event.is_set():
                        break
                    self.machine.advance(rid)
            except Exception:  # noqa: BLE001 - never kill the worker
                log.exception("worker tick failed")
            self._stop_event.wait(self.interval)
