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
