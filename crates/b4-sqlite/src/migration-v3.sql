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
