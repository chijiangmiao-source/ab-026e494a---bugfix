"""Receipt construction and verification shared by mirror repos and control.

A receipt is the repository-side evidence for one prepare/activate operation.
It is HMAC-signed with the repo secret so the control service can verify that
the evidence genuinely comes from the expected repository.
"""
from __future__ import annotations

import hashlib
import hmac
import uuid
from datetime import datetime, timezone


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _payload(receipt: dict) -> str:
    return "|".join(
        [
            str(receipt["receipt_id"]),
            str(receipt["repo"]),
            str(receipt["op"]),
            str(receipt["op_key"]),
            str(receipt["digest"]),
            str(receipt["ts"]),
        ]
    )


def new_receipt(repo: str, op: str, op_key: str, digest: str, secret: str) -> dict:
    receipt = {
        "receipt_id": uuid.uuid4().hex,
        "repo": repo,
        "op": op,
        "op_key": op_key,
        "digest": digest,
        "ts": utcnow(),
    }
    receipt["sig"] = hmac.new(
        secret.encode(), _payload(receipt).encode(), hashlib.sha256
    ).hexdigest()
    return receipt


def verify_receipt(secret: str, receipt: dict) -> bool:
    if not isinstance(receipt, dict):
        return False
    try:
        expected = hmac.new(
            secret.encode(), _payload(receipt).encode(), hashlib.sha256
        ).hexdigest()
    except (KeyError, TypeError):
        return False
    return hmac.compare_digest(expected, str(receipt.get("sig", "")))
