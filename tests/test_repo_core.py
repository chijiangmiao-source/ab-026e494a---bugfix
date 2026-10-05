import hashlib
import os
import tempfile
import unittest

from app.common.receipts import verify_receipt
from app.repo.core import Conflict, DisconnectedAfterCommit, NotPrepared, RepoCore


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


class RepoCoreTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.core = RepoCore(os.path.join(self.dir.name, "repo.db"), "repo-a", "secret-a")

    def tearDown(self):
        self.core.close()
        self.dir.cleanup()

    def test_prepare_then_replay_returns_first_receipt(self):
        r1, created = self.core.prepare("k1", sha(b"x"), b"x")
        self.assertTrue(created)
        self.assertTrue(verify_receipt("secret-a", r1))
        r2, created = self.core.prepare("k1", sha(b"x"), b"x")
        self.assertFalse(created)
        self.assertEqual(r1["receipt_id"], r2["receipt_id"])

    def test_prepare_same_key_different_digest_conflicts(self):
        self.core.prepare("k1", sha(b"x"), b"x")
        with self.assertRaises(Conflict) as ctx:
            self.core.prepare("k1", sha(b"y"), b"y")
        self.assertEqual(ctx.exception.existing_digest, sha(b"x"))

    def test_prepare_rejects_digest_mismatch(self):
        with self.assertRaises(ValueError):
            self.core.prepare("k1", sha(b"x"), b"different-bytes")

    def test_activate_requires_prepare(self):
        with self.assertRaises(NotPrepared):
            self.core.activate("k2", sha(b"x"))

    def test_activate_flips_pointer_exactly_once(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        r1, created = self.core.activate("ka", sha(b"x"))
        self.assertTrue(created)
        self.assertEqual(self.core.state()["active_digest"], sha(b"x"))
        self.assertEqual(self.core.state()["activation_count"], 1)
        r2, created = self.core.activate("ka", sha(b"x"))
        self.assertFalse(created)
        self.assertEqual(r1["receipt_id"], r2["receipt_id"])
        self.assertEqual(self.core.state()["activation_count"], 1)

    def test_activate_same_key_different_digest_conflicts(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.prepare("kp2", sha(b"y"), b"y")
        self.core.activate("ka", sha(b"x"))
        with self.assertRaises(Conflict):
            self.core.activate("ka", sha(b"y"))

    def test_state_survives_reopen(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.activate("ka", sha(b"x"))
        self.core.close()
        self.core = RepoCore(os.path.join(self.dir.name, "repo.db"), "repo-a", "secret-a")
        self.assertEqual(self.core.state()["active_digest"], sha(b"x"))
        self.assertEqual(self.core.get_op("ka")["digest"], sha(b"x"))

    def test_disconnect_after_activate_commits_then_drops(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.arm_disconnect_after_activate()
        with self.assertRaises(DisconnectedAfterCommit):
            self.core.activate("ka", sha(b"x"))
        # The activation was committed even though the response was dropped.
        self.assertIsNotNone(self.core.get_op("ka"))
        self.assertEqual(self.core.state()["active_digest"], sha(b"x"))
        self.assertTrue(self.core.disconnected)
        # After recovery the same op key replays the first receipt, no re-activation.
        self.core.set_disconnected(False)
        r, created = self.core.activate("ka", sha(b"x"))
        self.assertFalse(created)
        self.assertEqual(self.core.state()["activation_count"], 1)

    def test_corrupt_next_activate_returns_foreign_digest_without_state_change(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.arm_corrupt_next_activate()
        receipt, created = self.core.activate("ka", sha(b"x"))
        self.assertTrue(created)
        self.assertNotEqual(receipt["digest"], sha(b"x"))
        self.assertTrue(verify_receipt("secret-a", receipt))  # well-formed but foreign
        state = self.core.state()
        self.assertIsNone(state["active_digest"])
        self.assertEqual(state["activation_count"], 0)
        self.assertIsNone(self.core.get_op("ka"))  # nothing persisted

    def test_rollback_on_fresh_volume_restores_null_pointer(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.activate("ka", sha(b"x"))
        self.assertEqual(self.core.state()["active_digest"], sha(b"x"))
        self.assertTrue(self.core.rollback_activate("ka"))
        state = self.core.state()
        self.assertIsNone(state["active_digest"])
        self.assertEqual(state["activation_count"], 0)
        # Idempotent: repeated rollback keeps the null pointer.
        self.assertTrue(self.core.rollback_activate("ka"))
        state = self.core.state()
        self.assertIsNone(state["active_digest"])
        self.assertEqual(state["activation_count"], 0)

    def test_rollback_restores_previous_pointer(self):
        self.core.prepare("kp0", sha(b"old"), b"old")
        self.core.activate("ka0", sha(b"old"))
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.activate("ka", sha(b"x"))
        self.assertEqual(self.core.state()["active_digest"], sha(b"x"))
        self.assertTrue(self.core.rollback_activate("ka"))
        state = self.core.state()
        self.assertEqual(state["active_digest"], sha(b"old"))
        self.assertEqual(state["activation_count"], 1)

    def test_rollback_unknown_or_prepare_key(self):
        self.assertFalse(self.core.rollback_activate("nope"))
        self.core.prepare("kp", sha(b"x"), b"x")
        # A prepare key is not an activation and cannot be rolled back.
        self.assertFalse(self.core.rollback_activate("kp"))

    def test_rollback_does_not_revert_a_newer_activation(self):
        self.core.prepare("kp1", sha(b"x"), b"x")
        self.core.activate("ka1", sha(b"x"))
        self.core.prepare("kp2", sha(b"y"), b"y")
        self.core.activate("ka2", sha(b"y"))
        # Rolling back the older activation must not clobber the newer one.
        self.assertTrue(self.core.rollback_activate("ka1"))
        state = self.core.state()
        self.assertEqual(state["active_digest"], sha(b"y"))

    def test_rollback_survives_reopen(self):
        self.core.prepare("kp", sha(b"x"), b"x")
        self.core.activate("ka", sha(b"x"))
        self.core.close()
        self.core = RepoCore(os.path.join(self.dir.name, "repo.db"), "repo-a", "secret-a")
        self.assertTrue(self.core.rollback_activate("ka"))
        self.assertIsNone(self.core.state()["active_digest"])


if __name__ == "__main__":
    unittest.main()
