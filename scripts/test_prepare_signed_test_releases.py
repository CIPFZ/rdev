"""Test fixture public keys must satisfy the unchanged strict policy contract."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("prepare_signed", Path(__file__).with_name("prepare-signed-test-releases.py"))
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PublicKeyTest(unittest.TestCase):
    def test_actual_generated_key_comment_is_removed_without_changing_identity(self):
        with tempfile.TemporaryDirectory(prefix="rdev-test-public-key-") as directory:
            key = Path(directory) / "test-only"
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C",
                            "phase8 ephemeral test only", "-f", str(key)], check=True, timeout=10)
            public = key.with_suffix(".pub").read_text()
            normalized = prepare.policy_public_key(public)
            self.assertEqual(len(normalized.split()), 2)
            self.assertNotIn("\n", normalized)
            policy_key = Path(directory) / "policy.pub"
            policy_key.write_text(normalized + "\n")
            def fingerprint(path):
                return subprocess.check_output(["ssh-keygen", "-lf", str(path), "-E", "sha256"],
                                               text=True, timeout=10).split()[1]
            self.assertEqual(fingerprint(key.with_suffix(".pub")), fingerprint(policy_key))

    def test_options_wrong_algorithm_and_multiple_records_are_refused(self):
        for public in ("", "ssh-ed25519", "ssh-rsa AAAA comment",
                       'command="false" ssh-ed25519 AAAA',
                       "ssh-ed25519 AAAA\nssh-ed25519 BBBB"):
            with self.subTest(public=public):
                with self.assertRaises(ValueError):
                    prepare.policy_public_key(public)


if __name__ == "__main__":
    unittest.main()
