"""Pure validation and key-derivation logic for the control service."""
from __future__ import annotations

import base64
import binascii
import hashlib
import re

MAX_ARTIFACT_BYTES = 64 * 1024  # 64KiB
MAX_B64_CHARS = MAX_ARTIFACT_BYTES * 2  # generous bound on the encoded form

RELEASE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

STATE_PENDING = "PENDING"
STATE_PREPARING = "PREPARING"
STATE_ACTIVATING = "ACTIVATING"
STATE_COMPLETED = "COMPLETED"
STATE_REJECTED = "REJECTED"
TERMINAL_STATES = (STATE_COMPLETED, STATE_REJECTED)

OP_PREPARE = "prepare"
OP_ACTIVATE = "activate"


class ValidationError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def validate_release_id(release_id) -> str:
    if not isinstance(release_id, str) or not RELEASE_ID_RE.match(release_id):
        raise ValidationError(
            "invalid_release_id",
            "发布标识非法：需为 1-128 位字母、数字或 . _ -，且以字母或数字开头",
        )
    return release_id


def decode_artifact(b64) -> bytes:
    if not isinstance(b64, str) or not b64.strip():
        raise ValidationError("invalid_base64", "工件必须是 Base64 字符串")
    compact = re.sub(r"\s+", "", b64)
    if len(compact) > MAX_B64_CHARS:
        raise ValidationError("artifact_too_large", "工件超过 64KiB 上限", status=413)
    try:
        data = base64.b64decode(compact, validate=True)
    except (binascii.Error, ValueError):
        raise ValidationError("invalid_base64", "工件不是合法 Base64")
    if len(data) > MAX_ARTIFACT_BYTES:
        raise ValidationError("artifact_too_large", "工件超过 64KiB 上限", status=413)
    return data


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def op_key(release_id: str, repo: str, op: str) -> str:
    """Repository-side idempotency key derived from the release identifier."""
    return f"rel:{release_id}:{repo}:{op}"
