"""Mirror repository HTTP service (one process per offline mirror)."""
from __future__ import annotations

import base64
import binascii
import logging
import os
import re
import signal
import threading

from app.common.httpjson import ApiError, App, make_server
from app.repo.core import (
    Conflict,
    DisconnectedAfterCommit,
    NotPrepared,
    RepoCore,
)

log = logging.getLogger("repo")

MAX_ARTIFACT_BYTES = 64 * 1024


def _decode_artifact(b64) -> bytes:
    if not isinstance(b64, str) or not b64.strip():
        raise ApiError(400, "invalid_base64", "工件必须是 Base64 字符串")
    compact = re.sub(r"\s+", "", b64)
    try:
        data = base64.b64decode(compact, validate=True)
    except (binascii.Error, ValueError):
        raise ApiError(400, "invalid_base64", "工件不是合法 Base64")
    if len(data) > MAX_ARTIFACT_BYTES:
        raise ApiError(413, "artifact_too_large", "工件超过 64KiB 上限")
    return data


def build_app(core: RepoCore, fault_hooks: bool = False) -> App:
    app = App(core.name)

    def guard():
        if core.disconnected:
            raise ApiError(503, "disconnected", "镜像仓已断开响应")

    @app.route("GET", "/healthz")
    def healthz(req):
        guard()
        return {"status": "ok", "repo": core.name}

    @app.route("POST", "/v1/prepare")
    def prepare(req):
        guard()
        body = req.json()
        op_key, digest = body.get("op_key"), body.get("digest")
        if not (isinstance(op_key, str) and op_key and isinstance(digest, str) and digest):
            raise ApiError(400, "bad_request", "op_key 与 digest 为必填字符串")
        artifact = _decode_artifact(body.get("artifact_b64"))
        try:
            receipt, created = core.prepare(op_key, digest, artifact)
        except ValueError:
            raise ApiError(400, "digest_mismatch", "工件摘要与声明不符")
        except Conflict as e:
            raise ApiError(409, "op_key_conflict", "操作键已绑定不同摘要",
                           {"existing_digest": e.existing_digest})
        return (201 if created else 200), {"receipt": receipt}

    @app.route("POST", "/v1/activate")
    def activate(req):
        guard()
        body = req.json()
        op_key, digest = body.get("op_key"), body.get("digest")
        if not (isinstance(op_key, str) and op_key and isinstance(digest, str) and digest):
            raise ApiError(400, "bad_request", "op_key 与 digest 为必填字符串")
        try:
            receipt, created = core.activate(op_key, digest)
        except NotPrepared:
            raise ApiError(400, "not_prepared", "该摘要尚未在仓内准备")
        except Conflict as e:
            raise ApiError(409, "op_key_conflict", "操作键已绑定不同摘要",
                           {"existing_digest": e.existing_digest})
        except DisconnectedAfterCommit:
            # The activation IS committed; only the response is dropped.
            raise ApiError(503, "disconnected", "镜像仓已断开响应")
        return (201 if created else 200), {"receipt": receipt}

    @app.route("GET", "/v1/ops/{op_key}")
    def get_op(req):
        guard()
        receipt = core.get_op(req.params["op_key"])
        if receipt is None:
            raise ApiError(404, "not_found", "操作键不存在")
        return {"receipt": receipt}

    @app.route("GET", "/v1/state")
    def state(req):
        guard()
        return core.state()

    if fault_hooks:

        @app.route("POST", "/fault/disconnect")
        def fault_disconnect(req):
            core.set_disconnected(True)
            return {"disconnected": True}

        @app.route("POST", "/fault/recover")
        def fault_recover(req):
            core.set_disconnected(False)
            return {"disconnected": False}

        @app.route("POST", "/fault/disconnect-after-activate")
        def fault_disconnect_after_activate(req):
            core.arm_disconnect_after_activate()
            return {"armed": "disconnect-after-activate"}

        @app.route("POST", "/fault/corrupt-next-activate")
        def fault_corrupt_next_activate(req):
            core.arm_corrupt_next_activate()
            return {"armed": "corrupt-next-activate"}

        @app.route("GET", "/fault/state")
        def fault_state(req):
            return core.fault_state()

    return app


class RepoService:
    def __init__(self, *, data_dir, name, secret, host="0.0.0.0", port=8001,
                 fault_hooks=False):
        self.core = RepoCore(os.path.join(data_dir, "repo.db"), name, secret)
        self.app = build_app(self.core, fault_hooks=fault_hooks)
        self.httpd = make_server(self.app, host, port)
        self._thread = None

    @property
    def port(self) -> int:
        return self.httpd.server_address[1]

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self.httpd.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.core.close()

    def serve_forever(self) -> None:
        self.httpd.serve_forever(poll_interval=0.2)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    svc = RepoService(
        data_dir=os.environ.get("DATA_DIR", "/data"),
        name=os.environ.get("REPO_NAME", "repo-a"),
        secret=os.environ.get("REPO_SECRET", "dev-secret"),
        port=int(os.environ.get("PORT", "8001")),
        fault_hooks=os.environ.get("FAULT_HOOKS", "") == "1",
    )

    def _shutdown(signum, frame):
        log.info("received signal %s, shutting down", signum)
        threading.Thread(target=svc.httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    log.info("repo %s listening on :%d", svc.core.name, svc.port)
    try:
        svc.serve_forever()
    finally:
        svc.core.close()


if __name__ == "__main__":
    main()
