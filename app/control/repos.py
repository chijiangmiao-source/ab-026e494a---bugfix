"""HTTP client for the offline mirror repositories."""
from __future__ import annotations

import urllib.parse

from app.common.httpjson import http_json


class RepoClient:
    def __init__(self, name: str, base_url: str, timeout: float = 3.0):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def get_op(self, op_key: str):
        key = urllib.parse.quote(op_key, safe="")
        return http_json("GET", f"{self.base_url}/v1/ops/{key}", timeout=self.timeout)

    def prepare(self, op_key: str, digest: str, artifact_b64: str):
        return http_json(
            "POST",
            f"{self.base_url}/v1/prepare",
            {"op_key": op_key, "digest": digest, "artifact_b64": artifact_b64},
            timeout=self.timeout,
        )

    def activate(self, op_key: str, digest: str):
        return http_json(
            "POST",
            f"{self.base_url}/v1/activate",
            {"op_key": op_key, "digest": digest},
            timeout=self.timeout,
        )

    def rollback(self, op_key: str):
        return http_json(
            "POST",
            f"{self.base_url}/v1/rollback",
            {"op_key": op_key},
            timeout=self.timeout,
        )

    def state(self):
        return http_json("GET", f"{self.base_url}/v1/state", timeout=self.timeout)
