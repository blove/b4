use super::report::Projection;
use super::source::Reader;
use super::InspectionError;
use b4_contract::{Snapshot, ThreadId};
use rusqlite::{config::DbConfig, limits::Limit, types::ValueRef, Connection, ErrorCode};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

const MAX_EXECUTIONS: usize = 256;
const MAX_SNAPSHOT: usize = 1024 * 1024;
const MAX_TOTAL_SNAPSHOTS: usize = 16 * 1024 * 1024;
const MAX_SCHEMA: usize = 32;
const MAX_DDL: usize = 64 * 1024;
const MAX_DEPTH: usize = 64;
const MAX_NODES: usize = 16_384;

pub(super) struct Budget {
    start: Instant,
    callbacks: AtomicU64,
    exhausted: AtomicBool,
    max_callbacks: u64,
    elapsed: Duration,
}

impl Budget {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            callbacks: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
            max_callbacks: 2000,
            elapsed: Duration::from_secs(5),
        })
    }

    #[cfg(test)]
    pub(super) fn with_callbacks(max_callbacks: u64) -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            callbacks: AtomicU64::new(0),
            exhausted: AtomicBool::new(false),
            max_callbacks,
            elapsed: Duration::from_secs(5),
        })
    }

    pub(super) fn check(&self) -> Result<(), InspectionError> {
        if self.exhausted.load(Ordering::Relaxed) || self.start.elapsed() >= self.elapsed {
            self.exhausted.store(true, Ordering::Relaxed);
            return Err(InspectionError::LimitExceeded);
        }
        Ok(())
    }

    fn progress(&self) -> bool {
        let previous = self
            .callbacks
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            })
            .unwrap_or(u64::MAX);
        if previous >= self.max_callbacks.saturating_sub(1) {
            self.exhausted.store(true, Ordering::Relaxed);
        }
        self.check().is_err()
    }

    fn sql(&self, error: rusqlite::Error, fallback: InspectionError) -> InspectionError {
        match error.sqlite_error_code() {
            Some(ErrorCode::TooBig) => InspectionError::LimitExceeded,
            Some(ErrorCode::OperationInterrupted) if self.exhausted.load(Ordering::Relaxed) => {
                InspectionError::LimitExceeded
            }
            Some(ErrorCode::OperationInterrupted) => InspectionError::Unavailable,
            _ => fallback,
        }
    }
}

/// Safe controls are installed and read back before any SQL prepare.
pub(super) fn configure(
    connection: &Connection,
    budget: &Arc<Budget>,
    source: bool,
) -> Result<(), InspectionError> {
    budget.check()?;
    for (category, limit) in [
        (Limit::SQLITE_LIMIT_LENGTH, 2 * 1024 * 1024),
        (Limit::SQLITE_LIMIT_SQL_LENGTH, 16 * 1024),
        (Limit::SQLITE_LIMIT_COLUMN, 64),
        (Limit::SQLITE_LIMIT_EXPR_DEPTH, 64),
        (Limit::SQLITE_LIMIT_PARSER_DEPTH, 128),
        (Limit::SQLITE_LIMIT_ATTACHED, 0),
        (Limit::SQLITE_LIMIT_WORKER_THREADS, 0),
    ] {
        connection
            .set_limit(category, limit)
            .map_err(|_| InspectionError::Unavailable)?;
        if connection
            .limit(category)
            .map_err(|_| InspectionError::Unavailable)?
            != limit
        {
            return Err(InspectionError::Unavailable);
        }
    }
    for (config, value) in [
        (DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true),
        (DbConfig::SQLITE_DBCONFIG_TRUSTED_SCHEMA, false),
    ] {
        if connection
            .set_db_config(config, value)
            .map_err(|_| InspectionError::Unavailable)?
            != value
            || connection
                .db_config(config)
                .map_err(|_| InspectionError::Unavailable)?
                != value
        {
            return Err(InspectionError::Unavailable);
        }
    }
    let progress = Arc::clone(budget);
    connection
        .progress_handler(1000, Some(move || progress.progress()))
        .map_err(|_| InspectionError::Unavailable)?;
    // These fixed guarded statements are the first prepared source statements.
    if source {
        connection
            .pragma_update(None, "query_only", "ON")
            .map_err(|e| budget.sql(e, InspectionError::CorruptSource))?;
    }
    connection
        .pragma_update(None, "temp_store", "MEMORY")
        .map_err(|e| budget.sql(e, InspectionError::CorruptSource))?;
    connection
        .pragma_update(None, "cache_size", -256)
        .map_err(|e| budget.sql(e, InspectionError::CorruptSource))?;
    if source
        && connection
            .pragma_query_value(None, "query_only", |row| row.get::<_, i64>(0))
            .map_err(|e| budget.sql(e, InspectionError::CorruptSource))?
            != 1
    {
        return Err(InspectionError::Unavailable);
    }
    if connection
        .pragma_query_value(None, "temp_store", |row| row.get::<_, i64>(0))
        .map_err(|e| budget.sql(e, InspectionError::CorruptSource))?
        != 2
        || connection
            .pragma_query_value(None, "cache_size", |row| row.get::<_, i64>(0))
            .map_err(|e| budget.sql(e, InspectionError::CorruptSource))?
            != -256
    {
        return Err(InspectionError::Unavailable);
    }
    budget.check()
}

type SchemaObject = (String, String, String, Option<String>);

fn text(value: ValueRef<'_>) -> Result<&str, InspectionError> {
    match value {
        ValueRef::Text(bytes) => {
            std::str::from_utf8(bytes).map_err(|_| InspectionError::CorruptSource)
        }
        _ => Err(InspectionError::CorruptSource),
    }
}

fn schema_objects(
    connection: &Connection,
    budget: &Budget,
    cap: usize,
) -> Result<Vec<SchemaObject>, InspectionError> {
    budget.check()?;
    let map = |e| budget.sql(e, InspectionError::UnsupportedSchema);
    let mut statement = connection
        .prepare("SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name LIMIT ?1")
        .map_err(map)?;
    let mut rows = statement
        .query([i64::try_from(cap + 1).map_err(|_| InspectionError::LimitExceeded)?])
        .map_err(map)?;
    let mut objects = Vec::new();
    while let Some(row) = rows.next().map_err(map)? {
        if objects.len() == cap {
            return Err(InspectionError::LimitExceeded);
        }
        let mut fields = Vec::with_capacity(3);
        for index in 0..3 {
            let field = text(row.get_ref(index).map_err(map)?)?;
            if field.len() > MAX_DDL {
                return Err(InspectionError::LimitExceeded);
            }
            fields.push(field.to_owned());
        }
        let sql = match row.get_ref(3).map_err(map)? {
            ValueRef::Null => None,
            value => {
                let sql = text(value)?;
                if sql.len() > MAX_DDL {
                    return Err(InspectionError::LimitExceeded);
                }
                Some(sql.to_owned())
            }
        };
        objects.push((fields.remove(0), fields.remove(0), fields.remove(0), sql));
    }
    budget.check()?;
    Ok(objects)
}

fn schema(reader: &mut Reader, budget: &Arc<Budget>) -> Result<i64, InspectionError> {
    let actual = schema_objects(reader.connection(), budget, MAX_SCHEMA)?;
    reader.open_expected()?;
    let result = (|| {
        let expected = reader.expected();
        configure(expected, budget, false)?;
        expected
            .execute_batch(crate::SCHEMA_V1)
            .map_err(|e| budget.sql(e, InspectionError::Unavailable))?;
        expected
            .execute_batch(crate::MIGRATION_V2)
            .map_err(|e| budget.sql(e, InspectionError::Unavailable))?;
        expected
            .execute_batch(crate::MIGRATION_V3)
            .map_err(|e| budget.sql(e, InspectionError::Unavailable))?;
        if actual == schema_objects(expected, budget, MAX_SCHEMA)? {
            return Ok(crate::LEGACY_SCHEMA_VERSION);
        }
        expected
            .execute_batch(crate::MIGRATION_V4)
            .map_err(|e| budget.sql(e, InspectionError::Unavailable))?;
        if actual != schema_objects(expected, budget, MAX_SCHEMA)? {
            return Err(InspectionError::UnsupportedSchema);
        }
        Ok(crate::SCHEMA_VERSION)
    })();
    // No statements escape the reconstruction scope.
    reader.close_expected()?;
    result
}

fn unsigned(value: ValueRef<'_>) -> Result<u64, InspectionError> {
    match value {
        ValueRef::Blob(bytes) => Ok(u64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| InspectionError::CorruptSource)?,
        )),
        _ => Err(InspectionError::CorruptSource),
    }
}

fn metadata(
    connection: &Connection,
    budget: &Budget,
    schema_version: i64,
) -> Result<u64, InspectionError> {
    let map = |e| budget.sql(e, InspectionError::CorruptSource);
    let mut statement = connection
        .prepare("SELECT id, schema_version, migration_checksum, owner_epoch FROM metadata LIMIT 2")
        .map_err(map)?;
    let mut rows = statement.query([]).map_err(map)?;
    let row = rows
        .next()
        .map_err(map)?
        .ok_or(InspectionError::CorruptSource)?;
    if row.get::<_, i64>(0).map_err(map)? != 1 {
        return Err(InspectionError::CorruptSource);
    }
    if row.get::<_, i64>(1).map_err(map)? != schema_version {
        return Err(InspectionError::UnsupportedSchema);
    }
    let expected_schema = match schema_version {
        crate::LEGACY_SCHEMA_VERSION => crate::SCHEMA_V3,
        crate::SCHEMA_VERSION => crate::SCHEMA,
        _ => return Err(InspectionError::UnsupportedSchema),
    };
    if text(row.get_ref(2).map_err(map)?)? != crate::checksum(expected_schema) {
        return Err(InspectionError::UnsupportedSchema);
    }
    let owner = unsigned(row.get_ref(3).map_err(map)?)?;
    if owner == 0 || rows.next().map_err(map)?.is_some() {
        return Err(InspectionError::CorruptSource);
    }
    Ok(owner)
}

// Nonempty permission records remain unsupported until their own bounded
// decoder, recorded-reference consistency and budget gates are qualified.
fn permission_tables_empty(
    connection: &Connection,
    budget: &Budget,
) -> Result<(), InspectionError> {
    let map = |e| budget.sql(e, InspectionError::CorruptSource);
    for sql in [
        "SELECT EXISTS(SELECT 1 FROM permission_acceptances LIMIT 1)",
        "SELECT EXISTS(SELECT 1 FROM permission_decisions LIMIT 1)",
        "SELECT EXISTS(SELECT 1 FROM permission_uses LIMIT 1)",
    ] {
        budget.check()?;
        let exists: bool = connection
            .query_row(sql, [], |row| row.get(0))
            .map_err(map)?;
        if exists {
            return Err(InspectionError::UnsupportedSchema);
        }
        budget.check()?;
    }
    Ok(())
}

fn integrity(connection: &Connection, budget: &Budget) -> Result<(), InspectionError> {
    let map = |e| budget.sql(e, InspectionError::CorruptSource);
    {
        let mut statement = connection.prepare("PRAGMA quick_check(1)").map_err(map)?;
        let mut rows = statement.query([]).map_err(map)?;
        let row = rows
            .next()
            .map_err(map)?
            .ok_or(InspectionError::CorruptSource)?;
        if text(row.get_ref(0).map_err(map)?)? != "ok" || rows.next().map_err(map)?.is_some() {
            return Err(InspectionError::CorruptSource);
        }
    }
    {
        let mut statement = connection
            .prepare("PRAGMA foreign_key_check")
            .map_err(map)?;
        if statement
            .query([])
            .map_err(map)?
            .next()
            .map_err(map)?
            .is_some()
        {
            return Err(InspectionError::CorruptSource);
        }
    }
    let foreign_active: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM threads AS t LEFT JOIN executions AS e ON e.id = t.active_execution_id WHERE t.active_execution_id IS NOT NULL AND (e.id IS NULL OR e.thread_id <> t.id) LIMIT 1)", [], |row| row.get(0)).map_err(map)?;
    if foreign_active {
        return Err(InspectionError::CorruptSource);
    }
    budget.check()
}

pub(super) fn read(
    reader: &mut Reader,
    budget: &Arc<Budget>,
    blocked: bool,
) -> Result<Projection, InspectionError> {
    let schema_version = schema(reader, budget)?;
    let connection = reader.connection();
    let owner = metadata(connection, budget, schema_version)?;
    if schema_version == crate::SCHEMA_VERSION {
        permission_tables_empty(connection, budget)?;
    }
    integrity(connection, budget)?;
    let mut projection = Projection::new(blocked);
    if schema_version == crate::SCHEMA_VERSION {
        projection.mark_empty_schema4();
    }
    let mut total = 0usize;
    let map = |e| budget.sql(e, InspectionError::CorruptSource);
    {
        let mut statement = connection.prepare("SELECT id, thread_id, revision, owner_epoch, length(CAST(snapshot AS BLOB)), CASE WHEN length(CAST(snapshot AS BLOB)) <= 1048576 THEN snapshot ELSE NULL END FROM executions ORDER BY id LIMIT 257").map_err(map)?;
        let mut rows = statement.query([]).map_err(map)?;
        let mut count = 0usize;
        while let Some(row) = rows.next().map_err(map)? {
            if count == MAX_EXECUTIONS {
                return Err(InspectionError::LimitExceeded);
            }
            count = count.checked_add(1).ok_or(InspectionError::LimitExceeded)?;
            budget.check()?;
            let length = usize::try_from(row.get::<_, i64>(4).map_err(map)?)
                .map_err(|_| InspectionError::CorruptSource)?;
            if length > MAX_SNAPSHOT {
                return Err(InspectionError::LimitExceeded);
            }
            total = total
                .checked_add(length)
                .ok_or(InspectionError::LimitExceeded)?;
            if total > MAX_TOTAL_SNAPSHOTS {
                return Err(InspectionError::LimitExceeded);
            }
            let payload = text(row.get_ref(5).map_err(map)?)?;
            if payload.len() != length {
                return Err(InspectionError::CorruptSource);
            }
            let version = json_preflight(payload.as_bytes())?;
            if version.is_some_and(|version| version != u64::from(b4_contract::SNAPSHOT_VERSION)) {
                return Err(InspectionError::UnsupportedSnapshot);
            }
            let snapshot: Snapshot =
                serde_json::from_str(payload).map_err(|_| InspectionError::CorruptSource)?;
            budget.check()?;
            b4_kernel::validate_snapshot(&snapshot).map_err(|_| InspectionError::CorruptSource)?;
            budget.check()?;
            let id = text(row.get_ref(0).map_err(map)?)?;
            let thread = text(row.get_ref(1).map_err(map)?)?;
            if thread.len() > b4_contract::MAX_ID_BYTES {
                return Err(InspectionError::CorruptSource);
            }
            ThreadId::new(thread.to_owned()).map_err(|_| InspectionError::CorruptSource)?;
            let revision = unsigned(row.get_ref(2).map_err(map)?)?;
            let execution_owner = unsigned(row.get_ref(3).map_err(map)?)?;
            if snapshot.execution_id.as_str() != id
                || snapshot.revision != revision
                || snapshot.owner_generation != execution_owner
                || execution_owner == 0
                || execution_owner > owner
            {
                return Err(InspectionError::CorruptSource);
            }
            let exists: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM threads WHERE id = ?1)",
                    [thread],
                    |row| row.get(0),
                )
                .map_err(map)?;
            if !exists {
                return Err(InspectionError::CorruptSource);
            }
            projection.add(&snapshot)?;
            // The owned snapshot and all authority within it die before next row.
            drop(snapshot);
            budget.check()?;
        }
    }
    // Counts are fixed SQL scans under the cumulative engine budget, never a
    // substitute for the separately bounded execution enumeration.
    for (index, sql) in [
        "SELECT count(*) FROM metadata",
        "SELECT count(*) FROM threads",
        "SELECT count(*) FROM executions",
        "SELECT count(*) FROM journal",
        "SELECT count(*) FROM approval_grants",
        "SELECT count(*) FROM approval_acceptances",
    ]
    .into_iter()
    .enumerate()
    {
        let count: i64 = connection
            .query_row(sql, [], |row| row.get(0))
            .map_err(map)?;
        projection.tables[index] =
            u64::try_from(count).map_err(|_| InspectionError::CorruptSource)?;
        budget.check()?;
    }
    Ok(projection)
}

/// Nonallocating lexical shape guard. Syntax is then checked by Snapshot's
/// decoder; strings (including keys), containers, and primitives each count.
fn json_preflight(bytes: &[u8]) -> Result<Option<u64>, InspectionError> {
    let mut stack = [0u8; MAX_DEPTH];
    let mut depth = 0usize;
    let mut nodes = 0usize;
    let mut index = 0usize;
    let mut version_key = false;
    let mut version = None;
    while index < bytes.len() {
        let byte = bytes[index];
        match byte {
            b'{' | b'[' => {
                if depth == MAX_DEPTH {
                    return Err(InspectionError::LimitExceeded);
                }
                stack[depth] = byte;
                depth += 1;
                nodes = nodes.checked_add(1).ok_or(InspectionError::LimitExceeded)?;
                index += 1;
            }
            b'}' | b']' => {
                if depth == 0
                    || (byte == b'}' && stack[depth - 1] != b'{')
                    || (byte == b']' && stack[depth - 1] != b'[')
                {
                    return Err(InspectionError::CorruptSource);
                }
                depth -= 1;
                index += 1;
            }
            b'"' => {
                nodes = nodes.checked_add(1).ok_or(InspectionError::LimitExceeded)?;
                let start = index;
                index += 1;
                let mut escaped = false;
                let mut closed = false;
                while index < bytes.len() {
                    let current = bytes[index];
                    index += 1;
                    if escaped {
                        escaped = false;
                    } else if current == b'\\' {
                        escaped = true;
                    } else if current == b'"' {
                        closed = true;
                        break;
                    }
                }
                if !closed {
                    return Err(InspectionError::CorruptSource);
                }
                let mut after = index;
                while bytes.get(after).is_some_and(u8::is_ascii_whitespace) {
                    after += 1;
                }
                if depth == 1 && bytes.get(after) == Some(&b':') {
                    version_key = ascii_key(&bytes[start + 1..index - 1], b"schema_version");
                }
            }
            b'-' | b'0'..=b'9' | b't' | b'f' | b'n' => {
                nodes = nodes.checked_add(1).ok_or(InspectionError::LimitExceeded)?;
                let start = index;
                while bytes
                    .get(index)
                    .is_some_and(|b| !b.is_ascii_whitespace() && !matches!(*b, b',' | b']' | b'}'))
                {
                    index += 1;
                }
                if version_key && depth == 1 {
                    version = std::str::from_utf8(&bytes[start..index])
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok());
                    version_key = false;
                }
            }
            _ => {
                index += 1;
            }
        }
        if nodes > MAX_NODES {
            return Err(InspectionError::LimitExceeded);
        }
    }
    if depth != 0 {
        return Err(InspectionError::CorruptSource);
    }
    Ok(version)
}

fn ascii_key(mut bytes: &[u8], expected: &[u8]) -> bool {
    for expected in expected {
        if bytes.first() == Some(expected) {
            bytes = &bytes[1..];
        } else if bytes.len() >= 6 && bytes.starts_with(b"\\u00") {
            let Ok(hex) = std::str::from_utf8(&bytes[4..6]) else {
                return false;
            };
            if u8::from_str_radix(hex, 16).ok() != Some(*expected) {
                return false;
            }
            bytes = &bytes[6..];
        } else {
            return false;
        }
    }
    bytes.is_empty()
}

#[cfg(test)]
mod tests;
