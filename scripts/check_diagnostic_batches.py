#!/usr/bin/env python3
"""Validate authored embedding diagnostic batches; this is not log authenticity."""
from pathlib import Path
import json
import sys

from jsonschema import Draft202012Validator, SchemaError, ValidationError
from referencing.exceptions import Unresolvable

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "spec/runtime/request-diagnostic-batch.schema.json"
EXAMPLES = ROOT / "spec/runtime/diagnostic-examples"
MAX_BATCH_BYTES = 65_536
INVALID = "b4run_diagnostic_batch_invalid"


def validate_batch_bytes(data):
    def reject_number(_value):
        raise ValueError(INVALID)

    def unique_keys(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(INVALID)
            value[key] = item
        return value

    try:
        if (not isinstance(data, bytes) or not 0 < len(data) <= MAX_BATCH_BYTES
                or b"\n" in data or b"\r" in data):
            raise ValueError(INVALID)
        value = json.loads(
            data.decode("utf-8"), parse_float=reject_number,
            parse_constant=reject_number, object_pairs_hook=unique_keys,
        )
        schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        Draft202012Validator(schema).validate(value)
        if data != json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"):
            raise ValueError(INVALID)
        references = [record["request_id"] for record in value["records"]]
        if (len(set(references)) != len(references)
                or len({reference[:36] for reference in references}) > 1):
            raise ValueError(INVALID)
    except (UnicodeError, ValueError, TypeError, RecursionError, SchemaError,
            ValidationError, Unresolvable):
        raise ValueError(INVALID) from None


def main():
    try:
        for name in ["empty.json", "response.json", "dropped.json"]:
            validate_batch_bytes((EXAMPLES / name).read_bytes())
    except (OSError, ValueError):
        sys.stderr.write(INVALID + "\n")
        return 1
    print("Validated three authored request-diagnostic batches")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
