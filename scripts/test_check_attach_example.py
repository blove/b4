import json
import unittest

from check_attach_http_example import inactive_frames


STATE = b"event: state\ndata: " + json.dumps({"live": False, "turn": None, "values": {}}).encode() + b"\n\n"
RETRY = b"retry: 2000\n\n"
DONE = b'event: done\ndata: {"output":null}\n\n'


class InactiveBlockOrder(unittest.TestCase):
    def test_state_retry_done_allows_quiet_comment_blocks(self):
        self.assertEqual(inactive_frames(b": ping\n\n" + STATE + RETRY + b": ping\n\n" + DONE)["values"], {})

    def test_retry_before_state_is_rejected(self):
        with self.assertRaises(RuntimeError):
            inactive_frames(RETRY + STATE + DONE)

    def test_retry_after_done_is_rejected(self):
        with self.assertRaises(RuntimeError):
            inactive_frames(STATE + DONE + RETRY)

    def test_embedded_retry_and_duplicate_retry_are_rejected(self):
        for raw in [STATE[:-1] + RETRY + DONE, STATE + RETRY + RETRY + DONE]:
            with self.subTest(raw=raw), self.assertRaises(RuntimeError):
                inactive_frames(raw)


if __name__ == "__main__":
    unittest.main()
