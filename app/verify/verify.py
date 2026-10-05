"""One-shot acceptance service ("verify").

Runs to completion and exits; the process exit code is the acceptance result.

Phase order (per acceptance spec):
  A. Disconnect/restart convergence scenario: arm repo-b to drop its response
     right after committing the activation, restart the control service, then
     check both repos' final digests and the release evidence.
  B. Code tests (unittest discover).
  C. Build check (byte-compile every source file).
  D. HTTP smoke against the health page and the release API.
"""
from __future__ import annotations

import base64
import os
import subprocess
import sys
import time
import urllib.parse

from app.common.httpjson import TransportError, http_json, http_text
from app.control.core import op_key, sha256_hex

CONTROL_URL = os.environ.get("CONTROL_URL", "http://control:8080").rstrip("/")
REPO_A_URL = os.environ.get("REPO_A_URL", "http://repo-a:8001").rstrip("/")
REPO_B_URL = os.environ.get("REPO_B_URL", "http://repo-b:8002").rstrip("/")
SRC_DIR = os.environ.get("SRC_DIR", "/srv")

FAILURES: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""), flush=True)
    if not cond:
        FAILURES.append(name)


def wait_until(desc: str, pred, timeout: float = 30.0, interval: float = 0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            value = pred()
            if value:
                return value
        except Exception:
            pass
        time.sleep(interval)
    raise RuntimeError(f"timeout waiting for: {desc}")


def get_release(rid: str):
    return http_json("GET", f"{CONTROL_URL}/api/releases/{urllib.parse.quote(rid)}", timeout=5)


def repo_state(base_url: str) -> dict:
    status, body = http_json("GET", f"{base_url}/v1/state", timeout=5)
    if status != 200:
        raise RuntimeError(f"repo state unavailable: HTTP {status}")
    return body


# ---------------------------------------------------------------------------
# Phase A: disconnect + control-restart convergence scenario
# ---------------------------------------------------------------------------
def phase_scenario():
    print("== Phase A: 一仓激活后断响应 + 控制服务重启的收敛场景 ==", flush=True)
    rid = f"acc-{int(time.time())}"
    artifact = (f"calibration-bundle:{rid}:".encode() + bytes(range(256)) * 8)[:4096]
    sha = sha256_hex(artifact)
    b64 = base64.b64encode(artifact).decode()

    wait_until("control healthy",
               lambda: http_json("GET", f"{CONTROL_URL}/healthz", timeout=3)[0] == 200, 60)

    # repo-b will commit the activation, then drop the response and go dark.
    s, _ = http_json("POST", f"{REPO_B_URL}/fault/disconnect-after-activate", {}, timeout=5)
    check("arm repo-b disconnect-after-activate", s == 200, f"status={s}")

    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": rid, "artifact_b64": b64}, timeout=10)
    check("release accepted", s == 202 and b.get("sha256") == sha, f"status={s} body={b}")

    def stalled():
        s, b = get_release(rid)
        if s != 200:
            return None
        repos = b.get("repos") or {}
        a_act = (repos.get("repo-a") or {}).get("activate")
        b_act = (repos.get("repo-b") or {}).get("activate")
        if not (a_act and not b_act and b.get("state") != "COMPLETED"):
            return None
        s2, _ = http_json("GET", f"{REPO_B_URL}/v1/state", timeout=3)
        return b if s2 == 503 else None

    wait_until("repo-a activated while repo-b is dark", stalled, 45)
    s, _ = http_json("GET", f"{REPO_B_URL}/v1/state", timeout=5)
    check("repo-b stopped responding after its activation", s == 503, f"status={s}")

    # Restart the control service; it must converge from repo-side receipts.
    s, b = http_json("GET", f"{CONTROL_URL}/healthz", timeout=5)
    old_boot = b.get("boot_id")
    s, _ = http_json("POST", f"{CONTROL_URL}/fault/restart", {}, timeout=5)
    check("control restart hook accepted", s == 200, f"status={s}")

    def restarted():
        try:
            s, b = http_json("GET", f"{CONTROL_URL}/healthz", timeout=3)
        except TransportError:
            return False
        return s == 200 and b.get("boot_id") not in (None, old_boot)

    wait_until("control process actually restarted", restarted, 90, 0.3)
    check("control restarted with a new boot id", True)

    # repo-b is still dark: the release must NOT complete yet.
    time.sleep(2)
    s, b = get_release(rid)
    check("release not completed while repo-b is dark",
          s == 200 and b.get("state") != "COMPLETED", f"state={b.get('state')}")

    # repo-b recovers; control adopts its persisted activate receipt.
    s, _ = http_json("POST", f"{REPO_B_URL}/fault/recover", {}, timeout=5)
    check("repo-b recovered", s == 200, f"status={s}")

    def completed():
        s, b = get_release(rid)
        return b if s == 200 and b.get("state") == "COMPLETED" else None

    detail = wait_until("release converged to COMPLETED", completed, 45)

    check("final sha256 matches candidate bytes", detail.get("sha256") == sha)
    check("current digest equals release sha256", detail.get("current_digest") == sha,
          f"current={detail.get('current_digest')}")
    repos = detail.get("repos") or {}
    evidence_ok = True
    for repo in ("repo-a", "repo-b"):
        for op in ("prepare", "activate"):
            r = (repos.get(repo) or {}).get(op) or {}
            if r.get("digest") != sha or not r.get("receipt_id") or not r.get("sig"):
                evidence_ok = False
    check("prepare/activate evidence complete for both repos", evidence_ok)

    sa, sb = repo_state(REPO_A_URL), repo_state(REPO_B_URL)
    check("repo-a active digest == release sha256", sa.get("active_digest") == sha,
          f"active={sa.get('active_digest')}")
    check("repo-b active digest == release sha256", sb.get("active_digest") == sha,
          f"active={sb.get('active_digest')}")
    check("repo-b activated exactly once (no second activation)",
          sb.get("activation_count") == 1, f"count={sb.get('activation_count')}")
    check("repo-a activated exactly once", sa.get("activation_count") == 1,
          f"count={sa.get('activation_count')}")

    # The receipt control adopted is the repo's FIRST activate receipt.
    key = urllib.parse.quote(op_key(rid, "repo-b", "activate"), safe="")
    s, b = http_json("GET", f"{REPO_B_URL}/v1/ops/{key}", timeout=5)
    repo_receipt = b.get("receipt") or {}
    ctrl_receipt = (repos.get("repo-b") or {}).get("activate") or {}
    check("repo-b replayed its first activate receipt",
          s == 200 and repo_receipt.get("receipt_id") == ctrl_receipt.get("receipt_id"),
          f"repo={repo_receipt.get('receipt_id')} control={ctrl_receipt.get('receipt_id')}")
    return rid, artifact, sha


# ---------------------------------------------------------------------------
# Phase B: code tests
# ---------------------------------------------------------------------------
def phase_tests():
    print("== Phase B: 代码测试 ==", flush=True)
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-t", ".", "-v"],
        cwd=SRC_DIR, capture_output=True, text=True, timeout=600,
    )
    if proc.returncode != 0:
        print(proc.stdout[-4000:], flush=True)
        print(proc.stderr[-4000:], flush=True)
    check("unit + integration tests", proc.returncode == 0, f"rc={proc.returncode}")


# ---------------------------------------------------------------------------
# Phase C: build check
# ---------------------------------------------------------------------------
def phase_build():
    print("== Phase C: 构建检查 ==", flush=True)
    proc = subprocess.run(
        [sys.executable, "-m", "compileall", "-q", "app", "tests"],
        cwd=SRC_DIR, capture_output=True, text=True, timeout=300,
    )
    if proc.returncode != 0:
        print(proc.stderr[-4000:], flush=True)
    check("byte-compile all sources", proc.returncode == 0, f"rc={proc.returncode}")


# ---------------------------------------------------------------------------
# Phase D: HTTP smoke (health page + release API)
# ---------------------------------------------------------------------------
def phase_smoke(rid: str, artifact: bytes, sha: str):
    print("== Phase D: 健康页与发布接口的 HTTP 冒烟 ==", flush=True)
    b64 = base64.b64encode(artifact).decode()

    s, b = http_json("GET", f"{CONTROL_URL}/healthz", timeout=5)
    check("health endpoint responds", s == 200 and b.get("status") == "ok", f"status={s}")

    s, text = http_text("GET", f"{CONTROL_URL}/", timeout=5)
    check("console page served with feedback element",
          s == 200 and 'id="feedback"' in text and 'id="artifact"' in text and "发布" in text)

    # Duplicate submission: same id + same bytes -> replay, no second activation.
    a_before = repo_state(REPO_A_URL)["activation_count"]
    b_before = repo_state(REPO_B_URL)["activation_count"]
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": rid, "artifact_b64": b64}, timeout=10)
    check("duplicate submission replays current state",
          s == 200 and b.get("state") == "COMPLETED", f"status={s}")
    time.sleep(1.5)
    check("no second activation on repo-a",
          repo_state(REPO_A_URL)["activation_count"] == a_before)
    check("no second activation on repo-b",
          repo_state(REPO_B_URL)["activation_count"] == b_before)

    # Used id + different artifact -> 409, existing release state preserved.
    other = base64.b64encode(b"different-candidate-bytes").decode()
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": rid, "artifact_b64": other}, timeout=10)
    check("used id + different artifact rejected",
          s == 409 and (b.get("error") or {}).get("code") == "release_id_in_use",
          f"status={s} body={b}")
    s, b = get_release(rid)
    check("existing successful release state preserved",
          s == 200 and b.get("state") == "COMPLETED" and b.get("sha256") == sha,
          f"state={b.get('state')} sha={b.get('sha256')}")

    # Invalid Base64 -> 400 with feedback code.
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": f"{rid}-badb64", "artifact_b64": "%%%not-base64%%%"},
                     timeout=10)
    check("invalid base64 rejected with feedback",
          s == 400 and (b.get("error") or {}).get("code") == "invalid_base64",
          f"status={s} body={b}")

    # Oversized artifact (> 64KiB) -> 413 with feedback code.
    big = base64.b64encode(b"x" * (64 * 1024 + 1)).decode()
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": f"{rid}-big", "artifact_b64": big}, timeout=10)
    check("oversized artifact rejected with feedback",
          s == 413 and (b.get("error") or {}).get("code") == "artifact_too_large",
          f"status={s} body={b}")

    # Boundary: exactly 64KiB is accepted and completes.
    rid2 = f"{rid}-max"
    exact = bytes(64 * 1024)
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": rid2, "artifact_b64": base64.b64encode(exact).decode()},
                     timeout=10)
    check("64KiB boundary artifact accepted", s == 202, f"status={s} body={b}")

    def completed2():
        s, b = get_release(rid2)
        return b if s == 200 and b.get("state") == "COMPLETED" else None

    d2 = wait_until("boundary release completed", completed2, 45)
    check("boundary release digest matches", d2.get("sha256") == sha256_hex(exact))

    # Rejection: a repo returns a digest that does not belong to the release.
    rid3 = f"{rid}-rej"
    art3 = f"rejection-probe:{rid3}".encode()
    sha3 = sha256_hex(art3)
    a_state_before = repo_state(REPO_A_URL)
    b_state_before = repo_state(REPO_B_URL)
    s, _ = http_json("POST", f"{REPO_B_URL}/fault/corrupt-next-activate", {}, timeout=5)
    check("arm repo-b corrupt-next-activate", s == 200, f"status={s}")
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": rid3, "artifact_b64": base64.b64encode(art3).decode()},
                     timeout=10)
    check("rejection probe release accepted", s == 202, f"status={s}")

    def rejected():
        s, b = get_release(rid3)
        return b if s == 200 and b.get("state") == "REJECTED" else None

    d3 = wait_until("release locked as REJECTED", rejected, 45)
    check("rejection reason recorded", bool(d3.get("error")), f"error={d3.get('error')}")
    check("rejected release exposes no current digest",
          d3.get("current_digest") is None, f"current={d3.get('current_digest')}")
    a_state_after = repo_state(REPO_A_URL)
    b_state_after = repo_state(REPO_B_URL)
    # repo-b never switched; repo-a switched but MUST be rolled back to its
    # pre-release pointer -- the rejected artifact must never stay active.
    check("repo-b active pointer NOT rewritten",
          b_state_after.get("active_digest") == b_state_before.get("active_digest"),
          f"active={b_state_after.get('active_digest')}")
    check("repo-b did not register an activation for the rejected release",
          b_state_after.get("activation_count") == b_state_before.get("activation_count"))
    check("repo-a active pointer rolled back to its pre-release value",
          a_state_after.get("active_digest") == a_state_before.get("active_digest"),
          f"active={a_state_after.get('active_digest')}")
    check("repo-a did not keep an activation for the rejected release",
          a_state_after.get("activation_count") == a_state_before.get("activation_count"),
          f"count={a_state_after.get('activation_count')}")
    check("rejected artifact is active on NEITHER repo",
          a_state_after.get("active_digest") != sha3
          and b_state_after.get("active_digest") != sha3,
          f"a={a_state_after.get('active_digest')} b={b_state_after.get('active_digest')}")
    time.sleep(3)
    s, b = get_release(rid3)
    check("rejection is locked (state does not drift)", b.get("state") == "REJECTED",
          f"state={b.get('state')}")
    s, b = http_json("POST", f"{CONTROL_URL}/api/releases",
                     {"release_id": rid3, "artifact_b64": base64.b64encode(art3).decode()},
                     timeout=10)
    check("resubmission of a rejected release does not unlock it",
          s == 200 and b.get("state") == "REJECTED", f"status={s} state={b.get('state')}")
    check("still no activation after resubmission",
          repo_state(REPO_A_URL)["activation_count"] == a_state_before["activation_count"]
          and repo_state(REPO_B_URL)["activation_count"] == b_state_before["activation_count"])

    # Repo-level idempotency, exercised directly against repo-a.
    ts = int(time.time())
    key = f"rel:direct-{ts}:repo-a:prepare"
    d1 = sha256_hex(b"direct-one")
    a1 = base64.b64encode(b"direct-one").decode()
    s, b1 = http_json("POST", f"{REPO_A_URL}/v1/prepare",
                      {"op_key": key, "digest": d1, "artifact_b64": a1}, timeout=5)
    check("direct prepare accepted", s == 201, f"status={s}")
    s, b2 = http_json("POST", f"{REPO_A_URL}/v1/prepare",
                      {"op_key": key, "digest": d1, "artifact_b64": a1}, timeout=5)
    check("same key + same digest replays the first receipt",
          s in (200, 201)
          and (b2.get("receipt") or {}).get("receipt_id") == (b1.get("receipt") or {}).get("receipt_id"),
          f"status={s}")
    d2b = sha256_hex(b"direct-two")
    a2 = base64.b64encode(b"direct-two").decode()
    s, b3 = http_json("POST", f"{REPO_A_URL}/v1/prepare",
                      {"op_key": key, "digest": d2b, "artifact_b64": a2}, timeout=5)
    check("same key + different digest explicitly rejected",
          s == 409 and (b3.get("error") or {}).get("code") == "op_key_conflict",
          f"status={s} body={b3}")

    # Unknown release id -> 404.
    s, _ = get_release(f"{rid}-nope")
    check("unknown release id -> 404", s == 404, f"status={s}")


def main() -> int:
    print("verify: starting acceptance run", flush=True)
    started = time.time()
    rid = artifact = sha = None
    try:
        rid, artifact, sha = phase_scenario()
    except Exception as e:  # noqa: BLE001
        check("phase A (disconnect/restart scenario)", False, repr(e))
    for phase in (phase_tests, phase_build):
        try:
            phase()
        except Exception as e:  # noqa: BLE001
            check(phase.__name__, False, repr(e))
    if rid is not None:
        try:
            phase_smoke(rid, artifact, sha)
        except Exception as e:  # noqa: BLE001
            check("phase D (HTTP smoke)", False, repr(e))
    else:
        check("phase D (HTTP smoke)", False, "scenario phase failed, no completed release")

    elapsed = time.time() - started
    print(f"== verify: {'PASS' if not FAILURES else 'FAIL'} "
          f"({len(FAILURES)} failure(s)) in {elapsed:.1f}s ==", flush=True)
    for name in FAILURES:
        print(f"  failed: {name}", flush=True)
    return 0 if not FAILURES else 1


if __name__ == "__main__":
    sys.exit(main())
