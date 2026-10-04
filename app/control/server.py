"""Control service: console UI + release API + orchestration worker."""
from __future__ import annotations

import logging
import os
import signal
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from app.common.httpjson import ApiError, App, Html, make_server
from app.control import core
from app.control.machine import ReleaseMachine, Worker
from app.control.repos import RepoClient
from app.control.store import Store

log = logging.getLogger("control")

CONSOLE_HTML = Path(__file__).with_name("console.html").read_text(encoding="utf-8")


def release_view(store: Store, rel: dict, repo_names) -> dict:
    receipts = store.receipts_for(rel["release_id"])
    repos = {}
    activated = []
    for repo in repo_names:
        entry = receipts.get(repo, {})
        repos[repo] = {
            core.OP_PREPARE: entry.get(core.OP_PREPARE),
            core.OP_ACTIVATE: entry.get(core.OP_ACTIVATE),
        }
        act = entry.get(core.OP_ACTIVATE)
        activated.append(act.get("digest") if act else None)
    # The "current digest" is only real once both repos activated the same sha.
    current = rel["sha256"] if all(d == rel["sha256"] for d in activated) else None
    return {
        "release_id": rel["release_id"],
        "sha256": rel["sha256"],
        "size": rel["size"],
        "state": rel["state"],
        "error": rel["error"],
        "current_digest": current,
        "created_at": rel["created_at"],
        "updated_at": rel["updated_at"],
        "repos": repos,
    }


def build_app(store: Store, repo_names, boot_id: str,
              fault_hooks: bool = False, restart_delay: float = 0.4) -> App:
    app = App("control")

    @app.route("GET", "/")
    def console(req):
        return Html(CONSOLE_HTML)

    @app.route("GET", "/healthz")
    def healthz(req):
        return {"status": "ok", "service": "control", "boot_id": boot_id}

    @app.route("POST", "/api/releases")
    def create_release(req):
        body = req.json()
        try:
            rid = core.validate_release_id(body.get("release_id"))
            artifact = core.decode_artifact(body.get("artifact_b64"))
        except core.ValidationError as e:
            raise ApiError(e.status, e.code, e.message)
        sha = core.sha256_hex(artifact)
        existing = store.get_release(rid)
        if existing is not None:
            if existing["sha256"] == sha:
                # Idempotent replay: same identifier + same bytes -> the stored
                # state is returned and no second activation ever happens.
                return 200, release_view(store, existing, repo_names)
            raise ApiError(
                409,
                "release_id_in_use",
                f"发布标识 {rid} 已被使用且工件摘要不同；已保留既有发布的真实状态",
                {"state": existing["state"], "sha256": existing["sha256"]},
            )
        # Persist sha256 + the immutable release intent BEFORE any repo call.
        try:
            store.insert_release(rid, sha, artifact, core.STATE_PENDING)
        except sqlite3.IntegrityError:
            existing = store.get_release(rid)
            if existing and existing["sha256"] == sha:
                return 200, release_view(store, existing, repo_names)
            raise ApiError(409, "release_id_in_use", f"发布标识 {rid} 已被使用")
        log.info("release intent persisted: %s sha256=%s size=%d", rid, sha, len(artifact))
        return 202, release_view(store, store.get_release(rid), repo_names)

    @app.route("GET", "/api/releases")
    def list_releases(req):
        return {"releases": store.list_releases()}

    @app.route("GET", "/api/releases/{release_id}")
    def get_release(req):
        rid = req.params["release_id"]
        rel = store.get_release(rid)
        if rel is None:
            raise ApiError(404, "not_found", f"发布 {rid} 不存在")
        return release_view(store, rel, repo_names)

    if fault_hooks:

        @app.route("POST", "/fault/restart")
        def fault_restart(req):
            # Test hook: terminate the process so the supervisor restarts it;
            # recovery must converge from durable state + repo receipts.
            log.warning("fault hook: restarting control process")
            def _die():
                time.sleep(restart_delay)
                os._exit(1)
            threading.Thread(target=_die, daemon=True).start()
            return {"restarting": True}

    return app


class ControlService:
    def __init__(self, *, data_dir, repo_urls, repo_secrets, host="0.0.0.0",
                 port=8080, worker_interval=0.5, repo_timeout=3.0,
                 fault_hooks=False):
        self.boot_id = uuid.uuid4().hex
        self.repo_names = list(repo_urls.keys())
        self.store = Store(os.path.join(data_dir, "control.db"))
        clients = {
            name: RepoClient(name, url, timeout=repo_timeout)
            for name, url in repo_urls.items()
        }
        self.machine = ReleaseMachine(self.store, clients, repo_secrets)
        self.worker = Worker(self.store, self.machine, interval=worker_interval)
        self.app = build_app(self.store, self.repo_names, self.boot_id,
                             fault_hooks=fault_hooks)
        self.httpd = make_server(self.app, host, port)
        self._thread = None

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self.worker.start()
        self._thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self.worker.stop()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.worker.join(timeout=5)
        self.store.close()

    def serve_forever(self) -> None:
        self.worker.start()
        try:
            self.httpd.serve_forever(poll_interval=0.2)
        finally:
            self.worker.stop()
            self.worker.join(timeout=5)
            self.store.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    port = int(os.environ.get("PORT", "8080"))
    repo_urls = {
        "repo-a": os.environ.get("REPO_A_URL", "http://repo-a:8001"),
        "repo-b": os.environ.get("REPO_B_URL", "http://repo-b:8002"),
    }
    repo_secrets = {
        "repo-a": os.environ.get("REPO_A_SECRET", "dev-secret-a"),
        "repo-b": os.environ.get("REPO_B_SECRET", "dev-secret-b"),
    }
    svc = ControlService(
        data_dir=os.environ.get("DATA_DIR", "/data"),
        repo_urls=repo_urls,
        repo_secrets=repo_secrets,
        port=port,
        worker_interval=float(os.environ.get("WORKER_INTERVAL_S", "0.5")),
        repo_timeout=float(os.environ.get("REPO_TIMEOUT_S", "3.0")),
        fault_hooks=os.environ.get("FAULT_HOOKS", "") == "1",
    )

    def _shutdown(signum, frame):
        log.info("received signal %s, shutting down", signum)
        threading.Thread(target=svc.httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("control listening on :%d", port)
    svc.serve_forever()


if __name__ == "__main__":
    main()
