CREATE TABLE metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    schema_version INTEGER NOT NULL,
    migration_checksum TEXT NOT NULL,
    owner_epoch BLOB NOT NULL CHECK (length(owner_epoch) = 8)
) STRICT;

CREATE TABLE threads (
    id TEXT PRIMARY KEY,
    active_execution_id TEXT REFERENCES executions(id)
) STRICT;

CREATE TABLE executions (
    id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL REFERENCES threads(id),
    revision BLOB NOT NULL CHECK (length(revision) = 8),
    owner_epoch BLOB NOT NULL CHECK (length(owner_epoch) = 8),
    snapshot TEXT NOT NULL
) STRICT;

CREATE TABLE journal (
    sequence INTEGER PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('thread', 'admit', 'commit')),
    thread_id TEXT NOT NULL REFERENCES threads(id),
    execution_id TEXT REFERENCES executions(id),
    revision BLOB CHECK (revision IS NULL OR length(revision) = 8),
    owner_epoch BLOB NOT NULL CHECK (length(owner_epoch) = 8),
    status TEXT,
    updated_at_ms BLOB NOT NULL CHECK (length(updated_at_ms) = 8),
    payload TEXT,
    CHECK ((kind = 'thread' AND execution_id IS NULL AND revision IS NULL AND status IS NULL AND payload IS NULL)
        OR (kind IN ('admit', 'commit') AND execution_id IS NOT NULL AND revision IS NOT NULL AND status IS NOT NULL AND payload IS NOT NULL))
) STRICT;
