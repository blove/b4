"""Independent qualifiers using genuine native report2 examples."""
import hashlib
import stat
import unittest
from pathlib import Path

import check_inspection_reports as reports

SUBJECTS = {"summary-v2.json": {"bytes": 1184, "mode": "0o644", "path": str(reports.EXAMPLES / "summary-v2.json"), "sha256": "166e574c9213d786705a55d23676c253de5ccf7cb62cfeb82918a386a9400312"}, "blocked-v2.json": {"bytes": 2003, "mode": "0o644", "path": str(reports.EXAMPLES / "blocked-v2.json"), "sha256": "7742e7f93c3fea864a7d3b940b884ebd31d2ed342e12115205d1497c52157cde"}}


def qualified_native_bytes(name):
    expected = SUBJECTS[name]
    try:
        path = Path(expected["path"])
        metadata = path.lstat()
        raw = path.read_bytes()
        valid = (stat.S_ISREG(metadata.st_mode) and not path.is_symlink()
                 and stat.S_IMODE(metadata.st_mode) == 0o644
                 and len(raw) == expected["bytes"] <= 65537
                 and hashlib.sha256(raw).hexdigest() == expected["sha256"]
                 and raw.endswith(b"\n") and not raw.endswith(b"\n\n"))
        raw.decode("utf-8")
    except (OSError, UnicodeError):
        raise RuntimeError("native_report_qualifier_setup_failed") from None
    if not valid:
        raise RuntimeError("native_report_qualifier_setup_failed")
    return raw


class NativeReport2AcceptanceTests(unittest.TestCase):
    def accepts_native(self, name):
        raw = qualified_native_bytes(name)
        try:
            result = reports.validate_report_bytes(raw)
        except reports.ReportError:
            self.fail("genuine_native_report2_must_be_accepted")
        self.assertIsNone(result, "valid_native_report_checker_returns_normally")

    def test_native_summary_report2_is_accepted(self):
        self.accepts_native("summary-v2.json")

    def test_native_blocked_report2_is_accepted(self):
        self.accepts_native("blocked-v2.json")
