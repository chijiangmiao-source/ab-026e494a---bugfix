import base64
import unittest

from app.common.receipts import new_receipt, verify_receipt
from app.control import core


class ReleaseIdTests(unittest.TestCase):
    def test_valid_ids(self):
        for rid in ["a", "cal-2026-10-04", "A.b_c-1", "x" * 128, "9"]:
            self.assertEqual(core.validate_release_id(rid), rid)

    def test_invalid_ids(self):
        for rid in ["", None, 123, "-lead", ".lead", "has space", "汉字", "x" * 129, "a/b"]:
            with self.assertRaises(core.ValidationError):
                core.validate_release_id(rid)


class ArtifactTests(unittest.TestCase):
    def test_roundtrip(self):
        data = b"hello calibration"
        self.assertEqual(core.decode_artifact(base64.b64encode(data).decode()), data)

    def test_whitespace_tolerated(self):
        data = b"abc"
        enc = base64.b64encode(data).decode()
        self.assertEqual(core.decode_artifact(enc[:2] + "\n" + enc[2:] + " "), data)

    def test_invalid_base64(self):
        for bad in ["%%%", "abc!def", "====", 123, None, "", "   "]:
            with self.assertRaises(core.ValidationError) as ctx:
                core.decode_artifact(bad)
            self.assertEqual(ctx.exception.code, "invalid_base64")

    def test_exactly_64kib_ok(self):
        data = bytes(64 * 1024)
        self.assertEqual(core.decode_artifact(base64.b64encode(data).decode()), data)

    def test_oversize_rejected(self):
        data = bytes(64 * 1024 + 1)
        with self.assertRaises(core.ValidationError) as ctx:
            core.decode_artifact(base64.b64encode(data).decode())
        self.assertEqual(ctx.exception.code, "artifact_too_large")
        self.assertEqual(ctx.exception.status, 413)


class OpKeyTests(unittest.TestCase):
    def test_derivation_is_deterministic_and_scoped(self):
        k1 = core.op_key("rel-1", "repo-a", "prepare")
        self.assertIn("rel-1", k1)
        self.assertEqual(k1, core.op_key("rel-1", "repo-a", "prepare"))
        self.assertNotEqual(k1, core.op_key("rel-1", "repo-a", "activate"))
        self.assertNotEqual(k1, core.op_key("rel-1", "repo-b", "prepare"))
        self.assertNotEqual(k1, core.op_key("rel-2", "repo-a", "prepare"))


class ReceiptSigTests(unittest.TestCase):
    def test_roundtrip_and_tamper(self):
        r = new_receipt("repo-a", "prepare", "rel:x:repo-a:prepare", "ab" * 32, "s3cret")
        self.assertTrue(verify_receipt("s3cret", r))
        self.assertFalse(verify_receipt("wrong-secret", r))
        tampered = dict(r, digest="00" * 32)
        self.assertFalse(verify_receipt("s3cret", tampered))
        self.assertFalse(verify_receipt("s3cret", {"bogus": True}))


if __name__ == "__main__":
    unittest.main()
