"""Release state machine: drives prepare/activate across both mirror repos.

Crash-safety model: the release intent is persisted before any repo call, and
every repo receipt is persisted locally as soon as it is observed. Before
issuing an operation the machine first asks the repo for an existing receipt
under the derived op key, so a control-service restart converges from
repo-side receipts without ever re-executing an operation.

Rejection model: a release only becomes REJECTED once the activate outcome of
EVERY repo has been gathered. When one repo reports a digest that does not
belong to the release (or conflicting/invalid evidence), every other repo
that had already flipped its live active pointer to the candidate digest is
rolled back (durably and idempotently on the repo side) BEFORE the release
locks. A rejected candidate therefore never remains active anywhere, and a
crash mid-rollback simply resumes the rollback on the next worker tick.
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
    """Internal signal: the release has just been locked as REJECTED."""


class _ReceiptBad(Exception):
    """A repo's evidence conflicts with the release (bad sig / key conflict).

    Carries a reason but does NOT terminalize the release itself: during the
    activate phase all repos must be accounted for and any already-switched
    active pointers rolled back before REJECTED is locked.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


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
            self._run(rel)
        except Rejected:
            pass
        except Exception:  # noqa: BLE001 - keep the worker alive
            log.exception("advance failed for release %s", release_id)

    # ---- internals ----
    def _run(self, rel: dict) -> None:
        rid, sha = rel["release_id"], rel["sha256"]
        artifact_b64 = base64.b64encode(rel["artifact"]).decode()

        # Phase 1: prepare both repos with the identical candidate bytes.
        # A prepare never moves a live pointer, so conflicting evidence here
        # can lock REJECTED immediately without any rollback.
        for repo in self.clients:
            try:
                receipt = self._ensure_op(rid, repo, core.OP_PREPARE, sha, artifact_b64)
            except _ReceiptBad as bad:
                self._reject(rid, bad.reason)
                raise Rejected()
            if receipt is None:
                self._set_state(rid, core.STATE_PREPARING)
                return
            if receipt.get("digest") != sha:
                self._reject(
                    rid,
                    f"镜像仓 {repo} 的 prepare 回执摘要不属于本发布"
                    f"（收到 {receipt.get('digest')}，期望 {sha}），发布已锁定为拒绝",
                )
                raise Rejected()

        self._set_state(rid, core.STATE_ACTIVATING)

        # Phase 2: gather the activate outcome of EVERY repo before deciding.
        # A repo that already flipped its live pointer must be rolled back
        # before REJECTED is locked, so the release stays ACTIVATING (and the
        # worker retries after a restart) until rollback is confirmed.
        receipts: dict[str, dict] = {}
        bad_reason: str | None = None
        for repo in self.clients:
            try:
                receipt = self._ensure_op(rid, repo, core.OP_ACTIVATE, sha, artifact_b64)
            except _ReceiptBad as bad:
                bad_reason = bad.reason
                continue
            if receipt is None:
                return  # unreachable / transient: retry next tick
            receipts[repo] = receipt
            if receipt.get("digest") != sha and bad_reason is None:
                bad_reason = (
                    f"镜像仓 {repo} 的 activate 回执摘要不属于本发布"
                    f"（收到 {receipt.get('digest')}，期望 {sha}），发布已锁定为拒绝"
                )

        if bad_reason is not None:
            # Undo every activation that switched a pointer to THIS release's
            # digest. Foreign-digest receipts never moved a pointer.
            for repo, receipt in receipts.items():
                if receipt.get("digest") == sha and not self._rollback(rid, repo):
                    # Keep the release non-terminal; a later tick retries the
                    # idempotent rollback and then locks the rejection.
                    log.warning("rollback pending for release %s on repo %s", rid, repo)
                    return
            self._reject(rid, bad_reason)
            raise Rejected()

        self._set_state(rid, core.STATE_COMPLETED)
        log.info("release %s completed (sha256=%s)", rid, sha)

    def _rollback(self, rid: str, repo: str) -> bool:
        """Ask a repo to undo the release's activation; idempotent on retry."""
        key = core.op_key(rid, repo, core.OP_ACTIVATE)
        try:
            status, _ = self.clients[repo].rollback_activate(key)
        except TransportError as e:
            log.warning("repo %s rollback unreachable for %s: %s", repo, rid, e)
            return False
        if status in (200, 204):
            log.info("repo %s rolled back activation %s", repo, key)
            return True
        if status == 404:
            # Nothing committed under that key, so no live pointer to restore.
            return True
        log.warning("repo %s rollback for %s -> HTTP %s", repo, rid, status)
        return False

    def _ensure_op(self, rid: str, repo: str, op: str, sha: str,
                   artifact_b64: str) -> dict | None:
        """Return the repo receipt for this op, persisting it locally.

        Returns None when the repo is unreachable (the worker retries later).
        Raises _ReceiptBad when repo-side evidence conflicts with the release;
        the caller decides when (and after which rollbacks) to lock REJECTED.
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
                    raise self._conflict(rid, repo, op, key, body)
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
            raise _ReceiptBad(
                f"镜像仓 {repo} 的 {op} 证据签名校验失败，发布将锁定为拒绝"
            )
        self.store.put_receipt(rid, repo, op, key, str(receipt.get("digest", "")), receipt)
        return receipt

    def _conflict(self, rid: str, repo: str, op: str, key: str, body: dict) -> "_ReceiptBad":
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
        return _ReceiptBad(
            f"镜像仓 {repo} 拒绝了 {op}：操作键已绑定不同摘要（{existing}）"
        )

    def _reject(self, rid: str, message: str) -> None:
        log.error("release %s rejected: %s", rid, message)
        self._set_state(rid, core.STATE_REJECTED, error=message)

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
