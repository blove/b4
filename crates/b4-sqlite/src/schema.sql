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

ALTER TABLE threads ADD COLUMN created_at_ms BLOB NOT NULL DEFAULT x'0000000000000000' CHECK (length(created_at_ms) = 8);
ALTER TABLE threads ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}';
ALTER TABLE threads ADD COLUMN origin TEXT;
ALTER TABLE threads ADD COLUMN resource_revision BLOB NOT NULL DEFAULT x'0000000000000000' CHECK (length(resource_revision) = 8);
ALTER TABLE threads ADD COLUMN generation TEXT NOT NULL DEFAULT '0000000000000000000000000000000000000000000000000000000000000000' CHECK (length(generation) = 64 AND generation NOT GLOB '*[^0-9a-f]*');

CREATE TABLE approval_grants (
    execution_id TEXT NOT NULL REFERENCES executions(id),
    interrupt_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES threads(id),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    task_parking_revision BLOB NOT NULL CHECK (length(task_parking_revision) = 8),
    consumed INTEGER NOT NULL CHECK (consumed IN (0, 1)),
    record TEXT NOT NULL,
    PRIMARY KEY (execution_id, interrupt_id)
) STRICT;

CREATE TABLE approval_acceptances (
    execution_id TEXT NOT NULL REFERENCES executions(id),
    turn_id TEXT NOT NULL,
    thread_id TEXT NOT NULL REFERENCES threads(id),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    previous_revision BLOB NOT NULL CHECK (length(previous_revision) = 8),
    definition_revision TEXT NOT NULL,
    accepted_at_ms BLOB NOT NULL CHECK (length(accepted_at_ms) = 8),
    record TEXT NOT NULL,
    PRIMARY KEY (execution_id, turn_id)
) STRICT;

CREATE TABLE permission_acceptances (
    execution_id TEXT NOT NULL REFERENCES executions(id) CHECK (length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256),
    turn_id TEXT NOT NULL CHECK (length(CAST(turn_id AS BLOB)) BETWEEN 1 AND 256),
    thread_id TEXT NOT NULL REFERENCES threads(id) CHECK (length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    previous_revision BLOB NOT NULL CHECK (length(previous_revision) = 8),
    committed_revision BLOB NOT NULL CHECK (length(committed_revision) = 8),
    record_version INTEGER NOT NULL CHECK (record_version = 1),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 65536),
    PRIMARY KEY (execution_id, turn_id),
    UNIQUE (execution_id, previous_revision)
) STRICT;
CREATE INDEX permission_acceptances_by_thread ON permission_acceptances(thread_id, execution_id, turn_id);

CREATE TABLE permission_decisions (
    permission_id TEXT NOT NULL CHECK (length(CAST(permission_id AS BLOB)) BETWEEN 1 AND 256),
    thread_id TEXT NOT NULL REFERENCES threads(id) CHECK (length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    scope_digest TEXT NOT NULL CHECK (length(scope_digest) = 64 AND scope_digest NOT GLOB '*[^0-9a-f]*'),
    record_version INTEGER NOT NULL CHECK (record_version = 1),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 65536),
    PRIMARY KEY (permission_id),
    UNIQUE (thread_id, thread_generation, scope_digest)
) STRICT;
CREATE INDEX permission_decisions_by_thread ON permission_decisions(thread_id, thread_generation, permission_id);

CREATE TABLE permission_uses (
    execution_id TEXT NOT NULL REFERENCES executions(id) CHECK (length(CAST(execution_id AS BLOB)) BETWEEN 1 AND 256),
    task_id TEXT NOT NULL CHECK (length(CAST(task_id AS BLOB)) BETWEEN 1 AND 256),
    use_id TEXT NOT NULL CHECK (length(CAST(use_id AS BLOB)) BETWEEN 1 AND 256),
    thread_id TEXT NOT NULL REFERENCES threads(id) CHECK (length(CAST(thread_id AS BLOB)) BETWEEN 1 AND 256),
    thread_generation TEXT NOT NULL CHECK (length(thread_generation) = 64 AND thread_generation NOT GLOB '*[^0-9a-f]*'),
    permission_id TEXT NOT NULL REFERENCES permission_decisions(permission_id) CHECK (length(CAST(permission_id AS BLOB)) BETWEEN 1 AND 256),
    turn_id TEXT NOT NULL CHECK (length(CAST(turn_id AS BLOB)) BETWEEN 1 AND 256),
    authorization_revision BLOB NOT NULL CHECK (length(authorization_revision) = 8),
    record_version INTEGER NOT NULL CHECK (record_version = 1),
    record TEXT NOT NULL CHECK (length(CAST(record AS BLOB)) BETWEEN 1 AND 16384),
    PRIMARY KEY (execution_id, task_id),
    UNIQUE (use_id)
) STRICT;
CREATE INDEX permission_uses_by_thread ON permission_uses(thread_id, execution_id, task_id);
CREATE INDEX permission_uses_by_permission ON permission_uses(permission_id, execution_id, task_id);
