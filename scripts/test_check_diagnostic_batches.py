"""Behavioral qualification of the authored embedding diagnostic contract."""
import contextlib
import copy
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import check_diagnostic_batches as checker


def encode(value):
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


class DiagnosticBatchChecks(unittest.TestCase):
    def assert_valid(self, data):
        try:
            checker.validate_batch_bytes(data)
        except ValueError:
            self.fail("valid authored diagnostic batch was refused")

    def example(self, name="response.json"):
        return json.loads((checker.EXAMPLES / name).read_bytes())

    def test_all_three_authored_batch_forms_validate(self):
        for name in ["empty.json", "response.json", "dropped.json"]:
            data = (checker.EXAMPLES / name).read_bytes()
            self.assert_valid(data)

    def test_main_validates_examples_outside_the_ap_schema_inventory(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(checker.main(), 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(stdout.getvalue(), "Validated three authored request-diagnostic batches\n")

    def test_utf8_integer_and_duplicate_key_rules_are_strict(self):
        data = encode(self.example())
        for invalid in [
            data.decode().encode("utf-16-be"),
            data.replace(b'"elapsed_ms":0', b'"elapsed_ms":0.0'),
            data.replace(b'"elapsed_ms":0', b'"elapsed_ms":NaN'),
            data.replace(b'"elapsed_ms":0', b'"elapsed_ms":true'),
            data.replace(b'"redacted":true', b'"redacted":true,"redacted":true'),
        ]:
            with self.subTest(data_form=len(invalid)):
                with self.assertRaisesRegex(ValueError, f"^{checker.INVALID}$"):
                    checker.validate_batch_bytes(invalid)

    def test_unknown_or_private_fields_and_wrong_status_pairs_refuse(self):
        for change in [
            lambda x: x.update(private_path="private-batch-marker"),
            lambda x: x["records"][0].update(token="private-record-marker"),
            lambda x: x["records"][0].update(event="stream_finished"),
            lambda x: x["records"][0].update(method="patch"),
            lambda x: x["records"][0].update(status_code=None),
            lambda x: x["records"][0].update(event="request_future_dropped"),
            lambda x: x["records"][0].update(elapsed_ms=18446744073709551616),
            lambda x: x["records"][0].update(elapsed_ms=-1),
        ]:
            value = self.example()
            change(value)
            with self.assertRaisesRegex(ValueError, checker.INVALID):
                checker.validate_batch_bytes(encode(value))

    def test_request_id_syntax_requires_exact_ascii_and_nonzero_counter(self):
        for request_id in [
            "caller-request-marker", "b4r_" + "F" * 32 + "_" + "1" * 16,
            "b4r_" + "f" * 32 + "_" + "0" * 16,
            "b4r_" + "f" * 32 + "_" + "1" * 15,
        ]:
            value = self.example()
            value["records"][0]["request_id"] = request_id
            with self.assertRaisesRegex(ValueError, checker.INVALID):
                checker.validate_batch_bytes(encode(value))

    def test_batch_byte_bound_and_record_257_refuse(self):
        value = self.example()
        value["records"] *= 257
        for data in [encode(value), b" " * (checker.MAX_BATCH_BYTES + 1)]:
            with self.assertRaisesRegex(ValueError, checker.INVALID):
                checker.validate_batch_bytes(data)

    def test_counter_order_need_not_match_completion_order(self):
        value = self.example()
        later = copy.deepcopy(value["records"][0])
        later["request_id"] = "b4r_" + "0" * 32 + "_0000000000000002"
        value["records"].insert(0, later)
        self.assert_valid(encode(value))

    def test_one_batch_cannot_repeat_a_request_reference(self):
        value = self.example()
        value["records"].append(copy.deepcopy(value["records"][0]))
        with self.assertRaisesRegex(ValueError, f"^{checker.INVALID}$"):
            checker.validate_batch_bytes(encode(value))

    def test_one_batch_cannot_mix_host_namespaces(self):
        value = self.example()
        other = copy.deepcopy(value["records"][0])
        other["request_id"] = "b4r_" + "1" * 32 + "_0000000000000002"
        value["records"].append(other)
        with self.assertRaisesRegex(ValueError, f"^{checker.INVALID}$"):
            checker.validate_batch_bytes(encode(value))

    def test_encoded_batch_has_no_line_break_framing(self):
        value = self.example()
        for data in [encode(value) + b"\n", b"\r\n" + encode(value),
                     json.dumps(value, indent=2).encode("utf-8"),
                     b" " + encode(value), encode(value) + b"\t",
                     encode(value).replace(b'"records"', b'"recor\\u0064s"')]:
            with self.subTest(length=len(data)):
                with self.assertRaisesRegex(ValueError, f"^{checker.INVALID}$"):
                    checker.validate_batch_bytes(data)

    def test_compact_reordered_object_keys_remain_valid(self):
        value = self.example()
        value = dict(reversed(list(value.items())))
        value["records"][0] = dict(reversed(list(value["records"][0].items())))
        self.assert_valid(encode(value))

    def test_unresolvable_schema_reference_has_only_a_static_main_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            schema = Path(directory) / "schema.json"
            schema.write_text(json.dumps({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$ref": "#/private-reference-marker",
            }), encoding="utf-8")
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(checker, "SCHEMA", schema), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                try:
                    result = checker.main()
                except Exception as error:
                    self.fail(f"schema reference leaked {type(error).__name__} instead of static refusal")
            self.assertEqual(result, 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(stderr.getvalue(), checker.INVALID + "\n")

    def test_invalid_example_errors_do_not_echo_source_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            examples = Path(directory)
            for name in ["empty.json", "response.json", "dropped.json"]:
                (examples / name).write_bytes((checker.EXAMPLES / name).read_bytes())
            (examples / "response.json").write_bytes(b'{"private-marker":"secret-value"}')
            stdout, stderr = io.StringIO(), io.StringIO()
            with patch.object(checker, "EXAMPLES", examples), \
                    contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                self.assertEqual(checker.main(), 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(stderr.getvalue(), checker.INVALID + "\n")


if __name__ == "__main__":
    unittest.main()
