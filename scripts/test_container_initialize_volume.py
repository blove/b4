"""Refusal tests use real metadata and a private copy of the fixed-path script.

Only the fixed path changes in the copy. These tests qualify unchanged safe
refusals; actual UID0 acceptance and mutation are qualified separately on Linux.
"""

import os
from pathlib import Path
import shlex
import stat
import subprocess
import tempfile
import unittest


SOURCE = Path(__file__).with_name("container_initialize_volume.sh")


def tree_state(path):
    """Include names, bytes and metadata, without following a symlink."""
    if not path.exists() and not path.is_symlink():
        return None
    metadata = path.lstat()
    node = (
        stat.S_IFMT(metadata.st_mode),
        stat.S_IMODE(metadata.st_mode),
        metadata.st_uid,
        metadata.st_gid,
    )
    if path.is_symlink():
        return node, os.readlink(path)
    if path.is_dir():
        return node, tuple(
            (child.name, tree_state(child))
            for child in sorted(path.iterdir(), key=lambda child: child.name)
        )
    return node, path.read_bytes()


class InitializeVolumeTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="b4-volume-unit-")
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.root = self.base / "private root"

    def run_copy(self):
        text = SOURCE.read_text(encoding="utf-8")
        # This substitution affects the private copy only. Production accepts
        # no environment path or ownership override.
        text = text.replace(
            "volume_root=/var/lib/b4run",
            "volume_root=" + shlex.quote(str(self.root)),
        )
        copied = self.base / "initializer-copy.sh"
        copied.write_text(text, encoding="utf-8")
        environment = dict(os.environ)
        environment["PATH"] = os.defpath
        return subprocess.run(
            ["/bin/sh", str(copied)],
            env=environment,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )

    def assert_refusal(self, expected):
        before = tree_state(self.root)
        result = self.run_copy()
        self.assertEqual(result.returncode, 2)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, expected + "\n")
        self.assertEqual(tree_state(self.root), before)

    def test_nonroot_empty_root_refused_without_mutation(self):
        self.root.mkdir(mode=0o750)
        self.assert_refusal("b4run_volume_invalid_owner")

    def test_visible_existing_file_refused_without_changes(self):
        self.root.mkdir(mode=0o750)
        (self.root / "private-authority").write_bytes(b"preserve-existing-bytes")
        self.assert_refusal("b4run_volume_invalid_owner")

    def test_hidden_existing_file_refused_without_changes(self):
        self.root.mkdir(mode=0o710)
        (self.root / ".existing").write_bytes(b"hidden-private-bytes")
        self.assert_refusal("b4run_volume_invalid_owner")

    def test_nonroot_newline_only_child_preserved(self):
        self.root.mkdir(mode=0o700)
        (self.root / "\n").write_bytes(b"newline-name")
        self.assert_refusal("b4run_volume_invalid_owner")

    def test_empty_child_directory_is_nonempty(self):
        self.root.mkdir(mode=0o700)
        (self.root / "child").mkdir(mode=0o700)
        self.assert_refusal("b4run_volume_invalid_owner")

    def test_symlink_root_refused_without_target_changes(self):
        target = self.base / "target"
        target.mkdir(mode=0o700)
        (target / ".kept").write_bytes(b"untouched-target")
        self.root.symlink_to(target, target_is_directory=True)
        before = tree_state(target)
        self.assert_refusal("b4run_volume_invalid_root")
        self.assertEqual(tree_state(target), before)

    def test_regular_root_refused_without_changes(self):
        self.root.write_bytes(b"not-a-directory")
        self.assert_refusal("b4run_volume_invalid_root")

    def test_missing_root_refused_without_creation(self):
        self.assert_refusal("b4run_volume_invalid_root")

    def test_nonroot_existing_private_modes_preserved(self):
        self.root.mkdir(mode=0o750)
        kept = self.root / ".owner"
        kept.write_bytes(b"retained-owner-bytes")
        kept.chmod(0o600)
        self.assert_refusal("b4run_volume_invalid_owner")


if __name__ == "__main__":
    unittest.main()
