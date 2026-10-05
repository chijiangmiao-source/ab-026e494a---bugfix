"""In-process end-to-end tests: control service + two mirror repos."""
import base64
import hashlib
import os
import tempfile
import time
import unittest
import urllib.parse

from app.common.httpjson import http_json
from app.control.core import op_key
from app.control.machine import ReleaseMachine
from app.control.repos import RepoClient
from app.control.server import ControlService
from app.control.store import Store
from app.repo.server import RepoService


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def wait_for(pred, timeout=20.0, interval=0.05):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            value = pred()
            if value:
                return value
        except Exception:
            pass
        time.sleep(interval)
    raise AssertionError("timed out waiting for condition")


class Cluster:
    """A full deployment wired together on ephemeral localhost ports."""

    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        root = self.dir.name
        self.repo_a = RepoService(data_dir=os.path.join(root, "ra"), name="repo-a",
                                  secret="s-a", port=0, fault_hooks=True)
        self.repo_b = RepoService(data_dir=os.path.join(root, "rb"), name="repo-b",
                                  secret="s-b", port=0, fault_hooks=True)
        self.repo_a.start()
        self.repo_b.start()
        self.control_dir = os.path.join(root, "ctl")
        self.control = self._new_control()
        self.control.start()

    def _new_control(self) -> ControlService:
        return ControlService(
            data_dir=self.control_dir,
            repo_urls={"repo-a": self.repo_a.url, "repo-b": self.repo_b.url},
            repo_secrets={"repo-a": "s-a", "repo-b": "s-b"},
            port=0, worker_interval=0.05, repo_timeout=1.0, fault_hooks=True,
        )

    def restart_control(self):
        """Simulate a control-service restart over the same durable state."""
        self.control.stop()
        self.control = self._new_control()
        self.control.start()

    def close(self):
        self.control.stop()
        self.repo_a.stop()
        self.repo_b.stop()
        self.dir.cleanup()


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.c = Cluster()

    def tearDown(self):
        self.c.close()

    # ---- helpers ----
    def post_release(self, rid: str, artifact: bytes):
        return http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": rid, "artifact_b64": b64(artifact)}, timeout=5)

    def state_of(self, rid: str) -> dict:
        s, b = http_json("GET", f"{self.c.control.url}/api/releases/{rid}", timeout=5)
        return b if s == 200 else {}

    def wait_state(self, rid: str, state: str, timeout=20.0) -> dict:
        return wait_for(
            lambda: (lambda b: b if b.get("state") == state else None)(self.state_of(rid)),
            timeout,
        )

    def repo_state(self, repo) -> dict:
        return http_json("GET", f"{repo.url}/v1/state", timeout=5)[1]

    # ---- tests ----
    def test_happy_path_and_idempotent_duplicate(self):
        s, b = self.post_release("rel-1", b"payload-1")
        self.assertEqual(s, 202)
        self.assertEqual(b["sha256"], sha(b"payload-1"))
        d = self.wait_state("rel-1", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"payload-1"))
        for repo in ("repo-a", "repo-b"):
            self.assertEqual(d["repos"][repo]["prepare"]["digest"], sha(b"payload-1"))
            self.assertEqual(d["repos"][repo]["activate"]["digest"], sha(b"payload-1"))
        self.assertEqual(self.repo_state(self.c.repo_a)["active_digest"], sha(b"payload-1"))
        self.assertEqual(self.repo_state(self.c.repo_b)["active_digest"], sha(b"payload-1"))

        # Duplicate submission: same id + same bytes -> replay, no second activation.
        s, b = self.post_release("rel-1", b"payload-1")
        self.assertEqual(s, 200)
        self.assertEqual(b["state"], "COMPLETED")
        time.sleep(0.5)
        self.assertEqual(self.repo_state(self.c.repo_a)["activation_count"], 1)
        self.assertEqual(self.repo_state(self.c.repo_b)["activation_count"], 1)

    def test_used_id_with_different_artifact_preserves_state(self):
        self.post_release("rel-2", b"aaa")
        self.wait_state("rel-2", "COMPLETED")
        s, b = self.post_release("rel-2", b"bbb")
        self.assertEqual(s, 409)
        self.assertEqual(b["error"]["code"], "release_id_in_use")
        d = self.state_of("rel-2")
        self.assertEqual(d["state"], "COMPLETED")
        self.assertEqual(d["sha256"], sha(b"aaa"))

    def test_validation_feedback(self):
        s, b = http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": "rel-3", "artifact_b64": "%%%invalid%%%"}, timeout=5)
        self.assertEqual(s, 400)
        self.assertEqual(b["error"]["code"], "invalid_base64")

        big = base64.b64encode(bytes(64 * 1024 + 1)).decode()
        s, b = http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": "rel-3", "artifact_b64": big}, timeout=5)
        self.assertEqual(s, 413)
        self.assertEqual(b["error"]["code"], "artifact_too_large")

        s, b = http_json("POST", f"{self.c.control.url}/api/releases",
                         {"release_id": "bad id!", "artifact_b64": b64(b"x")}, timeout=5)
        self.assertEqual(s, 400)
        self.assertEqual(b["error"]["code"], "invalid_release_id")

        s, _ = http_json("GET", f"{self.c.control.url}/api/releases/rel-3", timeout=5)
        self.assertEqual(s, 404)

    def test_boundary_64kib_completes(self):
        s, _ = self.post_release("rel-4", bytes(64 * 1024))
        self.assertEqual(s, 202)
        d = self.wait_state("rel-4", "COMPLETED")
        self.assertEqual(d["sha256"], sha(bytes(64 * 1024)))

    def test_foreign_digest_locks_rejection_and_preserves_pointer(self):
        self.post_release("rel-5", b"first")
        self.wait_state("rel-5", "COMPLETED")
        before = self.repo_state(self.c.repo_b)

        http_json("POST", f"{self.c.repo_b.url}/fault/corrupt-next-activate", {}, timeout=5)
        self.post_release("rel-6", b"second")
        d = self.wait_state("rel-6", "REJECTED")
        self.assertIsNone(d["current_digest"])
        self.assertTrue(d["error"])

        after = self.repo_state(self.c.repo_b)
        self.assertEqual(after["active_digest"], before["active_digest"])
        self.assertEqual(after["activation_count"], before["activation_count"])

        time.sleep(0.5)
        self.assertEqual(self.state_of("rel-6")["state"], "REJECTED")  # locked
        s, b = self.post_release("rel-6", b"second")
        self.assertEqual(s, 200)
        self.assertEqual(b["state"], "REJECTED")

    def test_rejected_release_never_becomes_active_on_any_repo(self):
        # Exact regression: a brand-new cluster has NULL active pointers.
        # repo-b returns a signed-but-foreign activate receipt; repo-a had
        # already flipped. After REJECTED both repos must keep their prior
        # pointer and the rejected artifact must be active nowhere.
        artifact = b"rejected-candidate-bytes"
        art_sha = sha(artifact)
        before_a = self.repo_state(self.c.repo_a)
        before_b = self.repo_state(self.c.repo_b)
        self.assertIsNone(before_a["active_digest"])
        self.assertIsNone(before_b["active_digest"])

        s, _ = http_json("POST", f"{self.c.repo_b.url}/fault/corrupt-next-activate",
                         {}, timeout=5)
        self.assertEqual(s, 200)
        self.post_release("rel-rej-fresh", artifact)

        def rejected():
            d = self.state_of("rel-rej-fresh")
            return d if d.get("state") == "REJECTED" else None

        d = wait_for(rejected)
        self.assertIsNone(d["current_digest"])

        after_a = self.repo_state(self.c.repo_a)
        after_b = self.repo_state(self.c.repo_b)
        # Pointers preserved on BOTH repos ...
        self.assertEqual(after_a["active_digest"], before_a["active_digest"])
        self.assertEqual(after_b["active_digest"], before_b["active_digest"])
        # ... and never equal the rejected artifact's digest.
        self.assertNotEqual(after_a["active_digest"], art_sha)
        self.assertNotEqual(after_b["active_digest"], art_sha)
        # repo-a's activation was rolled back, not counted as a live activation
        # of the rejected candidate: its pointer is back at NULL.
        self.assertIsNone(after_a["active_digest"])
        self.assertIsNone(after_b["active_digest"])

        # Locked: no later tick flips either pointer.
        time.sleep(0.5)
        self.assertEqual(self.state_of("rel-rej-fresh")["state"], "REJECTED")
        self.assertIsNone(self.repo_state(self.c.repo_a)["active_digest"])
        self.assertIsNone(self.repo_state(self.c.repo_b)["active_digest"])

    def test_rejected_release_after_a_good_one_rolls_both_back(self):
        # Both repos already serve a live release; a rejected follow-up must
        # leave every pointer at the previous live version, not the candidate.
        self.post_release("rel-good", b"good-artifact")
        self.wait_state("rel-good", "COMPLETED")
        good_sha = sha(b"good-artifact")
        before_a = self.repo_state(self.c.repo_a)
        before_b = self.repo_state(self.c.repo_b)
        self.assertEqual(before_a["active_digest"], good_sha)
        self.assertEqual(before_b["active_digest"], good_sha)

        candidate = b"bad-followup-artifact"
        http_json("POST", f"{self.c.repo_b.url}/fault/corrupt-next-activate", {}, timeout=5)
        self.post_release("rel-bad-followup", candidate)
        wait_for(lambda: self.state_of("rel-bad-followup").get("state") == "REJECTED")

        after_a = self.repo_state(self.c.repo_a)
        after_b = self.repo_state(self.c.repo_b)
        self.assertEqual(after_a["active_digest"], before_a["active_digest"])
        self.assertEqual(after_b["active_digest"], before_b["active_digest"])
        self.assertNotEqual(after_a["active_digest"], sha(candidate))
        self.assertNotEqual(after_b["active_digest"], sha(candidate))


    def test_restart_converges_from_repo_receipts(self):
        http_json("POST", f"{self.c.repo_b.url}/fault/disconnect-after-activate", {}, timeout=5)
        s, _ = self.post_release("rel-7", b"third")
        self.assertEqual(s, 202)

        def stalled():
            d = self.state_of("rel-7")
            repos = d.get("repos") or {}
            a = (repos.get("repo-a") or {}).get("activate")
            bb = (repos.get("repo-b") or {}).get("activate")
            if not (a and not bb and d.get("state") != "COMPLETED"):
                return None
            s, _ = http_json("GET", f"{self.c.repo_b.url}/v1/state", timeout=2)
            return d if s == 503 else None

        wait_for(stalled)
        s, _ = http_json("GET", f"{self.c.repo_b.url}/v1/state", timeout=2)
        self.assertEqual(s, 503)  # repo-b is dark

        self.c.restart_control()
        time.sleep(0.5)
        self.assertNotEqual(self.state_of("rel-7").get("state"), "COMPLETED")

        http_json("POST", f"{self.c.repo_b.url}/fault/recover", {}, timeout=5)
        d = self.wait_state("rel-7", "COMPLETED")
        self.assertEqual(d["current_digest"], sha(b"third"))

        sb = self.repo_state(self.c.repo_b)
        self.assertEqual(sb["active_digest"], sha(b"third"))
        self.assertEqual(sb["activation_count"], 1)  # no second activation

        key = urllib.parse.quote(op_key("rel-7", "repo-b", "activate"), safe="")
        s, b = http_json("GET", f"{self.c.repo_b.url}/v1/ops/{key}", timeout=5)
        self.assertEqual(b["receipt"]["receipt_id"],
                         d["repos"]["repo-b"]["activate"]["receipt_id"])

    def test_restart_during_rejection_converges_rollback(self):
        # Drive the machine directly so we can crash control right after the
        # rejection decision, before rollback converges. repo-a has already
        # activated (flipping its pointer); the fresh control process must
        # roll it back on restart and only then lock REJECTED.
        root = self.c.dir.name
        ctl_dir = os.path.join(root, "ctl-reject-restart")
        store = Store(os.path.join(ctl_dir, "control.db"))
        clients = {
            "repo-a": RepoClient("repo-a", self.c.repo_a.url, timeout=2.0),
            "repo-b": RepoClient("repo-b", self.c.repo_b.url, timeout=2.0),
        }
        secrets = {"repo-a": "s-a", "repo-b": "s-b"}
        machine = ReleaseMachine(store, clients, secrets)

        rid, art = "rel-crash-reject", b"crash-reject-artifact"
        http_json("POST", f"{self.c.repo_b.url}/fault/corrupt-next-activate", {}, timeout=5)
        store.insert_release(rid, sha(art), art, "PENDING")
        machine.advance(rid)  # rejection decision parks as REJECTING, no lock yet
        self.assertEqual(store.get_release(rid)["state"], "REJECTING")
        # repo-a flipped before repo-b returned the foreign receipt ...
        self.assertEqual(self.repo_state(self.c.repo_a)["active_digest"], sha(art))

        # ... a brand-new control process resumes from durable state ...
        store.close()
        store2 = Store(os.path.join(ctl_dir, "control.db"))
        machine2 = ReleaseMachine(store2, clients, secrets)
        machine2.advance(rid)
        self.assertEqual(store2.get_release(rid)["state"], "REJECTED")

        # ... and leaves the rejected candidate active on neither repo.
        self.assertIsNone(self.repo_state(self.c.repo_a)["active_digest"])
        self.assertIsNone(self.repo_state(self.c.repo_b)["active_digest"])
        store2.close()

    def test_health_and_console_page(self):
        s, b = http_json("GET", f"{self.c.control.url}/healthz", timeout=5)
        self.assertEqual(s, 200)
        self.assertEqual(b["status"], "ok")
        self.assertTrue(b["boot_id"])
        from app.common.httpjson import http_text
        s, text = http_text("GET", f"{self.c.control.url}/", timeout=5)
        self.assertEqual(s, 200)
        self.assertIn('id="feedback"', text)
        self.assertIn('id="artifact"', text)


if __name__ == "__main__":
    unittest.main()
