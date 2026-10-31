ALTER TABLE threads ADD COLUMN created_at_ms BLOB NOT NULL DEFAULT x'0000000000000000' CHECK (length(created_at_ms) = 8);
ALTER TABLE threads ADD COLUMN metadata TEXT NOT NULL DEFAULT '{}';
ALTER TABLE threads ADD COLUMN origin TEXT;
ALTER TABLE threads ADD COLUMN resource_revision BLOB NOT NULL DEFAULT x'0000000000000000' CHECK (length(resource_revision) = 8);
