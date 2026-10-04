"""Minimal JSON HTTP server framework and client (standard library only)."""
from __future__ import annotations

import http.client
import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("httpjson")

MAX_BODY = 8 * 1024 * 1024


class ApiError(Exception):
    """An error that maps directly to an HTTP JSON error response."""

    def __init__(self, status: int, code: str, message: str, extra: dict | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.extra = extra or {}

    def body(self) -> dict:
        err = {"code": self.code, "message": self.message}
        err.update(self.extra)
        return {"error": err}


class TransportError(Exception):
    """Raised when an outbound HTTP call fails at the transport level."""


class Html(str):
    """Marker type: handler result should be served as text/html."""


class Request:
    def __init__(self, handler: "_Handler", params: dict):
        self._handler = handler
        self.params = params
        self.query = urllib.parse.parse_qs(urllib.parse.urlsplit(handler.path).query)
        self._body: bytes | None = None

    @property
    def body(self) -> bytes:
        if self._body is None:
            length = int(self._handler.headers.get("Content-Length") or 0)
            if length > MAX_BODY:
                raise ApiError(413, "request_too_large", "请求体过大")
            self._body = self._handler.rfile.read(length) if length else b""
        return self._body

    def json(self) -> dict:
        try:
            data = json.loads(self.body.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError(400, "bad_json", "请求体不是合法 JSON")
        if not isinstance(data, dict):
            raise ApiError(400, "bad_json", "请求体必须是 JSON 对象")
        return data


class App:
    def __init__(self, name: str):
        self.name = name
        self._routes: list[tuple[str, re.Pattern, object]] = []

    def add(self, method: str, pattern: str, fn) -> None:
        regex = re.compile("^" + re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern) + "$")
        self._routes.append((method.upper(), regex, fn))

    def route(self, method: str, pattern: str):
        def deco(fn):
            self.add(method, pattern, fn)
            return fn

        return deco


class _Handler(BaseHTTPRequestHandler):
    app: App = None  # type: ignore[assignment]
    server_version = "calibration/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # noqa: A003 - stdlib signature
        log.debug("%s - %s", self.address_string(), fmt % args)

    def _dispatch(self, method: str) -> None:
        path = urllib.parse.urlsplit(self.path).path
        try:
            for m, regex, fn in self.app._routes:
                if m != method:
                    continue
                match = regex.match(path)
                if match:
                    params = {
                        k: urllib.parse.unquote(v) for k, v in match.groupdict().items()
                    }
                    req = Request(self, params)
                    result = fn(req)
                    # Drain any unread request body so keep-alive stays sane.
                    if req._body is None:
                        try:
                            _ = req.body
                        except ApiError:
                            pass
                    self._reply(result)
                    return
            raise ApiError(404, "not_found", "接口不存在")
        except ApiError as e:
            self._reply((e.status, e.body()))
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:  # noqa: BLE001 - last-resort guard
            log.exception("unhandled error while serving %s %s", method, path)
            try:
                self._reply((500, {"error": {"code": "internal", "message": "服务内部错误"}}))
            except Exception:  # noqa: BLE001
                pass

    def _reply(self, result) -> None:
        if isinstance(result, tuple):
            status, payload = result
        else:
            status, payload = 200, result
        if isinstance(payload, Html):
            body = payload.encode("utf-8")
            ctype = "text/html; charset=utf-8"
        else:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            ctype = "application/json; charset=utf-8"
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")


def make_server(app: App, host: str, port: int) -> ThreadingHTTPServer:
    handler = type("BoundHandler", (_Handler,), {"app": app})
    srv = ThreadingHTTPServer((host, port), handler)
    srv.daemon_threads = True
    return srv


def http_json(method: str, url: str, payload: dict | None = None, timeout: float = 5.0):
    """Perform an HTTP JSON call; returns (status, parsed_body).

    Non-2xx responses are returned, not raised. Transport-level failures
    (DNS, refused, timeout, reset) raise TransportError.
    """
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {}
        return e.code, body
    except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
        raise TransportError(f"{method} {url}: {e}")


def http_text(method: str, url: str, timeout: float = 5.0):
    """Perform an HTTP call and return (status, text body)."""
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except (urllib.error.URLError, OSError, http.client.HTTPException) as e:
        raise TransportError(f"{method} {url}: {e}")
