#!/usr/bin/env python3
"""Private qualification-only SQLite inspection; never a runtime command."""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys

FIXED_DATA_DIR = Path("/var/lib/b4run/data")


class SnapshotError(Exception):
    """Only a fixed public failure code may escape inspection."""


# Explicit identities and primary-key ordering prevent schema drift, omitted
# columns, or rowid rearrangement from being hidden by a digest.
TABLES = (
    ("metadata", ("id", "schema_version", "migration_checksum", "owner_epoch"), ("id",)),
    ("threads", ("id", "active_execution_id", "created_at_ms", "metadata", "origin", "resource_revision", "generation"), ("id",)),
    ("executions", ("id", "thread_id", "revision", "owner_epoch", "snapshot"), ("id",)),
    ("journal", ("sequence", "kind", "thread_id", "execution_id", "revision", "owner_epoch", "status", "updated_at_ms", "payload"), ("sequence",)),
    ("approval_grants", ("execution_id", "interrupt_id", "task_id", "thread_id", "thread_generation", "task_parking_revision", "consumed", "record"), ("execution_id", "interrupt_id")),
    ("approval_acceptances", ("execution_id", "turn_id", "thread_id", "thread_generation", "previous_revision", "definition_revision", "accepted_at_ms", "record"), ("execution_id", "turn_id")),
)


def open_fixed_readonly_database(data_dir=FIXED_DATA_DIR, mode="live"):
    database = Path(data_dir) / "runtime.sqlite3"
    connection = None
    if mode not in ("live", "stopped"):
        raise SnapshotError("b4_snapshot_invalid_mode")
    try:
        if not stat.S_ISREG(database.lstat().st_mode):
            raise SnapshotError("b4_snapshot_invalid_database")
        if mode == "stopped" and any(os.path.lexists(str(database) + suffix)
                                     for suffix in ("-wal", "-shm", "-journal")):
            raise SnapshotError("b4_snapshot_sidecar_present")
        uri = database.resolve().as_uri() + "?mode=ro"
        if mode == "stopped":
            uri += "&immutable=1"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None, timeout=3)
        connection.execute("PRAGMA query_only=ON")
        if connection.execute("PRAGMA query_only").fetchone() != (1,):
            raise SnapshotError("b4_snapshot_readonly_failed")
        return connection
    except SnapshotError:
        if connection is not None:
            connection.close()
        raise
    except Exception:
        if connection is not None:
            connection.close()
        raise SnapshotError("b4_snapshot_open_failed") from None


def capture_private_snapshot(data_dir=FIXED_DATA_DIR, mode="live", after_table=None):
    connection = open_fixed_readonly_database(data_dir, mode)
    try:
        connection.execute("BEGIN DEFERRED")
        names = connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
        if names != sorted((name,) for name, _, _ in TABLES):
            raise SnapshotError("b4_snapshot_schema_mismatch")
        tables = []
        for name, columns, keys in TABLES:
            info = connection.execute(f"PRAGMA table_info({name})").fetchall()
            if tuple(row[1] for row in info) != columns:
                raise SnapshotError("b4_snapshot_schema_mismatch")
            rows = connection.execute(f"SELECT {','.join(columns)} FROM {name} ORDER BY {','.join(keys)}").fetchall()
            tables.append([name, list(columns), [[encode_canonical_cell(cell) for cell in row] for row in rows]])
            if after_table is not None:
                after_table(name)
        captured = {"format": "b4_private_snapshot_v1", "tables": tables}
        if tables[0][2] and (len(tables[0][2]) != 1 or tables[0][2][0][0] != ["integer", "1"]
                             or tables[0][2][0][1] != ["integer", "3"]):
            raise SnapshotError("b4_snapshot_schema_mismatch")
        if not tables[0][2]:
            raise SnapshotError("b4_snapshot_schema_mismatch")
        _owner_epoch(captured)
        return captured
    except SnapshotError:
        raise
    except Exception:
        raise SnapshotError("b4_snapshot_read_failed") from None
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


def encode_canonical_cell(value):
    if value is None:
        return ["null"]
    if type(value) is int:
        return ["integer", str(value)]
    if type(value) is str:
        return ["text", value]
    if type(value) is bytes:
        return ["blob", value.hex()]
    raise SnapshotError("b4_snapshot_invalid_cell")


def canonical_private_bytes(snapshot):
    return json.dumps(snapshot, ensure_ascii=True, separators=(",", ":")).encode("ascii")


def digest_private_snapshot(snapshot):
    """Internal only; authority-derived digests never enter public evidence."""
    return hashlib.sha256(canonical_private_bytes(snapshot)).hexdigest()


def _owner_epoch(snapshot):
    try:
        cell = snapshot["tables"][0][2][0][3]
        if cell[0] != "blob" or len(cell[1]) != 16:
            raise ValueError
        return int.from_bytes(bytes.fromhex(cell[1]), "big")
    except Exception:
        raise SnapshotError("b4_snapshot_invalid_epoch") from None


def compare_unchanged(before, after):
    return before == after


def compare_single_owner_reopen(before, after):
    try:
        old_epoch, new_epoch = _owner_epoch(before), _owner_epoch(after)
        if old_epoch == 2**64 - 1 or new_epoch != old_epoch + 1:
            return False
        expected = copy.deepcopy(before)
        expected["tables"][0][2][0][3] = ["blob", new_epoch.to_bytes(8, "big").hex()]
        return expected == after
    except SnapshotError:
        return False


def public_snapshot_summary(snapshot):
    return {"schema_version": 3, "table_counts": {name: len(rows) for name, _, rows in snapshot["tables"]}}


def decode_private_snapshot(raw):
    try:
        value = json.loads(raw)
        if set(value) != {"format", "tables"} or value["format"] != "b4_private_snapshot_v1" or len(value["tables"]) != 6:
            raise ValueError
        for table, (name, columns, _) in zip(value["tables"], TABLES):
            if len(table) != 3 or table[:2] != [name, list(columns)] or not isinstance(table[2], list):
                raise ValueError
            for row in table[2]:
                if len(row) != len(columns):
                    raise ValueError
                for cell in row:
                    if cell == ["null"]:
                        continue
                    if len(cell) != 2 or cell[0] not in ("integer", "text", "blob") or type(cell[1]) is not str:
                        raise ValueError
                    if cell[0] == "integer" and str(int(cell[1])) != cell[1]:
                        raise ValueError
                    if cell[0] == "blob" and bytes.fromhex(cell[1]).hex() != cell[1]:
                        raise ValueError
        _owner_epoch(value)
        return value
    except Exception:
        raise SnapshotError("b4_snapshot_invalid_private_payload") from None


class _Parser(argparse.ArgumentParser):
    def error(self, _message):
        raise SnapshotError("b4_snapshot_invalid_arguments")


def main(argv=None):
    parser = _Parser(add_help=False)
    parser.add_argument("--mode", choices=("live", "stopped"), required=True)
    args = parser.parse_args(argv)
    # stdout is a PRIVATE docker-exec captured pipe. This helper must never run
    # as a container PID1 whose raw output would become container application logs.
    sys.stdout.buffer.write(canonical_private_bytes(capture_private_snapshot(mode=args.mode)) + b"\n")


if __name__ == "__main__":
    try:
        main()
    except SnapshotError as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
    except Exception:
        print("b4_snapshot_failed", file=sys.stderr)
        sys.exit(2)
