//! Bounded physical capture and one dedicated settled-history deletion writer.
//! SQL may visit unrelated foreign decision rows; only materialized evidence is
//! bounded. No claim is made about whole-database scan time.
use super::*;
use b4_contract::{ApprovalDecision, Snapshot};
use b4_runtime::approval::{ApprovalAcceptance, GrantRecord};
use b4_runtime::permissions::{
    PermissionAcceptance, PermissionDecision, PermissionThreadDeletionAccount as Account,
    PermissionThreadDeletionCapture as Capture, PermissionThreadDeletionCommit as Commit,
    PermissionUse,
};
use b4_runtime::{JournalEntry, StoredExecution, ThreadDeletionTarget, ThreadView};
use rusqlite::{Connection, Transaction};

fn storage(_: rusqlite::Error) -> PermissionError {
    StoreError::Unavailable("permission_settled_thread_deletion_storage_failed".into()).into()
}
fn invalid() -> PermissionError {
    PermissionError::CorruptRecord
}
fn check(ok: bool) -> Result<(), PermissionError> {
    if ok {
        Ok(())
    } else {
        Err(invalid())
    }
}
#[derive(PartialEq)]
enum Cell {
    Null,
    Integer(i64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}
type PhysicalRow = Vec<Cell>;
fn bytes(cell: &Cell) -> Result<&[u8], PermissionError> {
    match cell {
        Cell::Text(v) => Ok(v),
        _ => Err(invalid()),
    }
}
fn string(cell: &Cell) -> Result<&str, PermissionError> {
    std::str::from_utf8(bytes(cell)?).map_err(|_| invalid())
}
fn number(cell: &Cell) -> Result<i64, PermissionError> {
    match cell {
        Cell::Integer(v) => Ok(*v),
        _ => Err(invalid()),
    }
}
fn u64cell(cell: &Cell) -> Result<u64, PermissionError> {
    match cell {
        Cell::Blob(v) => Ok(u64::from_be_bytes(
            v.as_slice().try_into().map_err(|_| invalid())?,
        )),
        _ => Err(invalid()),
    }
}
fn blobcell(cell: &Cell) -> Result<&[u8], PermissionError> {
    match cell {
        Cell::Blob(v) if v.len() == 8 => Ok(v),
        _ => Err(invalid()),
    }
}
fn optional(cell: &Cell) -> Result<Option<&[u8]>, PermissionError> {
    if matches!(cell, Cell::Null) {
        Ok(None)
    } else {
        bytes(cell).map(Some)
    }
}

// A LIMIT+1 sentinel is observed before copying a row. Each borrowed raw cell's
// storage class and size is checked before owning or decoding it. SQL headers
// cannot conceal contradictory stored payloads: all original cells are retained.
fn read_rows(
    connection: &Connection,
    sql: &str,
    target: &str,
    cap: usize,
    limits: &[usize],
    source: &mut Vec<u8>,
    account: &mut Account,
) -> Result<Vec<PhysicalRow>, PermissionError> {
    account.charge(1024, 1)?;
    let mut statement = connection.prepare(sql).map_err(storage)?;
    let mut rows = statement.query([target]).map_err(storage)?;
    let mut found = Vec::new();
    while let Some(row) = rows.next().map_err(storage)? {
        if found.len() == cap {
            return Err(PermissionError::LimitExceeded);
        }
        account.charge(256 + limits.len() * 32, 1)?;
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(limits.len())
            .map_err(|_| PermissionError::LimitExceeded)?;
        for (index, limit) in limits.iter().enumerate() {
            let raw = row.get_ref(index).map_err(storage)?;
            let (tag, value): (u8, &[u8]) = match raw {
                ValueRef::Null => (0, &[]),
                ValueRef::Integer(value) => {
                    account.append_cell(source, 1, &value.to_be_bytes())?;
                    cells.push(Cell::Integer(value));
                    continue;
                }
                ValueRef::Text(value) if value.len() <= *limit => (2, value),
                ValueRef::Blob(value) if value.len() <= *limit => (3, value),
                _ => return Err(invalid()),
            };
            account.append_cell(source, tag, value)?;
            let cell = match tag {
                0 => Cell::Null,
                2 => Cell::Text(value.to_vec()),
                _ => Cell::Blob(value.to_vec()),
            };
            cells.push(cell);
        }
        found.push(cells);
    }
    // End-of-range marker makes empty ranges and table boundaries observable.
    account.append_cell(source, 4, &(found.len() as u64).to_be_bytes())?;
    Ok(found)
}

fn metadata(
    connection: &Connection,
    owner: u64,
    source: &mut Vec<u8>,
    account: &mut Account,
) -> Result<(), PermissionError> {
    let rows = read_rows(connection,
        "SELECT schema_version, migration_checksum, owner_epoch FROM metadata WHERE id=1 AND ?1 IS NOT NULL LIMIT 2", "metadata", 1,
        &[8,64,8], source, account)?;
    check(rows.len() == 1)?;
    let row = &rows[0];
    if number(&row[0])? != crate::SCHEMA_VERSION
        || string(&row[1])? != crate::checksum(crate::SCHEMA)
    {
        return Err(StoreError::UnsupportedSchema.into());
    }
    if u64cell(&row[2])? != owner {
        return Err(StoreError::OwnerConflict.into());
    }
    Ok(())
}

fn exists(connection: &Connection, sql: &str, thread: &str) -> Result<bool, PermissionError> {
    connection
        .query_row(sql, [thread], |row| row.get(0))
        .map_err(storage)
}
fn reverse_relations(connection: &Connection, thread: &str) -> Result<(), PermissionError> {
    check(!exists(connection,
        "SELECT EXISTS(SELECT 1 FROM threads t JOIN executions e ON e.id=t.active_execution_id WHERE e.thread_id=?1 AND t.id!=?1)", thread)?)?;
    for table in [
        "journal",
        "approval_grants",
        "approval_acceptances",
        "permission_acceptances",
        "permission_uses",
    ] {
        let null_ok = table == "journal";
        let bad_execution = if null_ok {
            "(d.execution_id IS NOT NULL AND (e.thread_id IS NULL OR e.thread_id!=?1))"
        } else {
            "(e.thread_id IS NULL OR e.thread_id!=?1)"
        };
        check(!exists(connection, &format!(
            "SELECT EXISTS(SELECT 1 FROM {table} d LEFT JOIN executions e ON e.id=d.execution_id WHERE (d.thread_id=?1 OR e.thread_id=?1) AND (d.thread_id!=?1 OR {bad_execution}))"), thread)?)?;
    }
    // Existing permission index supplies the reverse Use->owned decision guard.
    check(!exists(connection,
        "SELECT EXISTS(SELECT 1 FROM permission_decisions d JOIN permission_uses u INDEXED BY permission_uses_by_permission ON u.permission_id=d.permission_id WHERE d.thread_id=?1 AND u.thread_id!=?1)", thread)?)?;
    // Creator is payload-only. Guard every JSON operation through lazy CASE.
    // Unknown, oversize or malformed foreign creator evidence refuses instead
    // of disappearing from a predicate. No unrelated record is materialized.
    check(!exists(connection, "SELECT EXISTS(SELECT 1 FROM permission_decisions d WHERE d.thread_id!=?1 AND CASE
        WHEN typeof(d.record)!='text' THEN 1
        WHEN length(CAST(d.record AS BLOB)) NOT BETWEEN 1 AND 65536 THEN 1
        WHEN NOT json_valid(d.record) THEN 1
        WHEN json_type(d.record) IS NOT 'object' THEN 1
        WHEN (SELECT count(*) FROM json_each(d.record) WHERE key='creator_execution_id')!=1 THEN 1
        WHEN json_type(d.record,'$.creator_execution_id') IS NOT 'text' THEN 1
        WHEN length(CAST(json_extract(d.record,'$.creator_execution_id') AS BLOB)) NOT BETWEEN 1 AND 256 THEN 1
        WHEN NOT EXISTS(SELECT 1 FROM executions e WHERE e.id=json_extract(d.record,'$.creator_execution_id')) THEN 1
        WHEN EXISTS(SELECT 1 FROM executions e WHERE e.thread_id=?1 AND e.id=json_extract(d.record,'$.creator_execution_id')) THEN 1
        ELSE 0 END)", thread)?)?;
    Ok(())
}

struct History {
    grants: Vec<GrantRecord>,
    ordinary: Vec<ApprovalAcceptance>,
    acceptances: Vec<PermissionAcceptance>,
    decisions: Vec<PermissionDecision>,
    uses: Vec<PermissionUse>,
    journal: Vec<JournalEntry>,
}
fn execution<'a>(
    executions: &'a [StoredExecution],
    id: &ExecutionId,
) -> Result<&'a StoredExecution, PermissionError> {
    executions
        .iter()
        .find(|e| &e.snapshot.execution_id == id)
        .ok_or_else(invalid)
}
fn point<'a>(
    entries: &'a [JournalEntry],
    id: &ExecutionId,
    revision: u64,
) -> Result<&'a JournalEntry, PermissionError> {
    entries
        .iter()
        .find(|e| &e.execution_id == id && e.revision == revision)
        .ok_or_else(invalid)
}
fn retained_before(
    before: &Snapshot,
    final_execution: &StoredExecution,
    journal: &[JournalEntry],
) -> Result<(), PermissionError> {
    let final_snapshot = &final_execution.snapshot;
    check(
        before.execution_id == final_snapshot.execution_id
            && before.definition == final_snapshot.definition
            && before.revision < final_snapshot.revision
            && before.owner_generation <= final_snapshot.owner_generation
            && before.updated_at_ms <= final_snapshot.updated_at_ms
            && point(journal, &before.execution_id, before.revision)?
                == &JournalEntry::from(before),
    )
}

fn validate_history(
    thread: &b4_runtime::ThreadRecord,
    executions: &[StoredExecution],
    h: &History,
    account: &mut Account,
) -> Result<(), PermissionError> {
    account.charge(32768, 128)?;
    for e in executions {
        let entries: Vec<_> = h
            .journal
            .iter()
            .filter(|j| j.execution_id == e.snapshot.execution_id)
            .collect();
        check(
            entries.first().is_some_and(|j| j.revision == 0)
                && entries.last().copied() == Some(&JournalEntry::from(&e.snapshot))
                && entries.windows(2).all(|pair| {
                    pair[0].revision.checked_add(1) == Some(pair[1].revision)
                        && pair[0].owner_generation <= pair[1].owner_generation
                        && pair[0].updated_at_ms <= pair[1].updated_at_ms
                }),
        )?;
    }
    for g in &h.grants {
        let e = &execution(executions, &g.execution_id)?.snapshot;
        check(
            thread.origin.as_ref() == Some(&g.origin)
                && g.route == e.definition.route
                && g.definition_revision == e.definition.revision
                && g.task_parking_revision <= e.revision
                && e.tasks
                    .iter()
                    .any(|t| t.spec.id == g.task_id && t.spec.effect_id == g.effect_id),
        )?;
    }
    for a in &h.ordinary {
        let e = &execution(executions, &a.execution_id)?.snapshot;
        check(
            thread.origin.as_ref() == Some(&a.context.actor)
                && a.definition_revision == e.definition.revision
                && a.previous_revision < e.revision
                && e.turn_history.contains(&a.turn_id),
        )?;
        let committed = point(
            &h.journal,
            &a.execution_id,
            a.previous_revision.checked_add(1).ok_or_else(invalid)?,
        )?;
        check(committed.updated_at_ms == a.accepted_at_ms)?;
        for d in &a.decisions {
            let g = h
                .grants
                .iter()
                .find(|g| g.execution_id == a.execution_id && g.interrupt_id == d.interrupt_id)
                .ok_or_else(invalid)?;
            check(
                g.consumed
                    && g.task_id == d.task_id
                    && g.task_parking_revision == d.task_parking_revision
                    && g.proposal_digest == d.proposal_digest
                    && g.policy_revision == a.context.policy_revision
                    && a.accepted_at_ms >= g.issued_at_ms
                    && a.accepted_at_ms < g.expires_at_ms,
            )?;
        }
    }
    for a in &h.acceptances {
        let e = execution(executions, &a.before_snapshot().execution_id)?;
        retained_before(a.before_snapshot(), e, &h.journal)?;
        check(
            a.thread_id() == &thread.id
                && a.thread_generation() == &thread.generation
                && thread.origin.as_ref() == Some(a.origin())
                && e.snapshot.turn_history.contains(a.turn_id()),
        )?;
        let committed = point(&h.journal, &e.snapshot.execution_id, a.committed_revision())?;
        check(
            committed.updated_at_ms == a.facts().now_ms
                && committed.owner_generation == a.before_snapshot().owner_generation,
        )?;
        for accepted in a.decisions() {
            let g = h
                .grants
                .iter()
                .find(|g| {
                    &g.execution_id == accepted.grant_ref().execution_id()
                        && &g.interrupt_id == accepted.grant_ref().interrupt_id()
                })
                .ok_or_else(invalid)?;
            let task = a
                .before_snapshot()
                .tasks
                .iter()
                .find(|t| &t.spec.id == accepted.task_id())
                .ok_or_else(invalid)?;
            let interrupt = task.interrupt.as_ref().ok_or_else(invalid)?;
            check(
                g.consumed
                    && &g.task_id == accepted.task_id()
                    && g.task_parking_revision == accepted.task_parking_revision()
                    && g.proposal_digest == accepted.proposal_digest()
                    && g.policy_revision == a.policy_revision()
                    && g.prompt == interrupt.prompt
                    && g.issued_at_ms <= a.facts().now_ms
                    && a.facts().now_ms < g.expires_at_ms,
            )?;
            if accepted.decision() == ApprovalDecision::Always {
                let d = h
                    .decisions
                    .iter()
                    .find(|d| Some(d.permission_id()) == accepted.permission_id())
                    .ok_or_else(invalid)?;
                let scope = d.scope();
                let binding = accepted.binding().ok_or_else(invalid)?;
                check(
                    d.creator_execution_id() == &a.before_snapshot().execution_id
                        && d.creator_turn_id() == a.turn_id()
                        && d.creator_task_id() == accepted.task_id()
                        && d.creator_interrupt_id() == accepted.interrupt_id()
                        && scope.thread_id() == a.thread_id()
                        && scope.thread_generation() == a.thread_generation()
                        && scope.origin() == a.origin()
                        && scope.application() == a.application()
                        && scope.route() == &a.before_snapshot().definition.route
                        && scope.definition() == &a.before_snapshot().definition
                        && scope.effect_id() == binding.effect_id()
                        && scope.tool_id() == binding.tool_id()
                        && scope.tool_revision() == binding.tool_revision()
                        && scope.policy_revision() == a.policy_revision()
                        && scope.constraint_revision() == binding.constraint_revision(),
                )?;
            }
        }
    }
    for d in &h.decisions {
        check(
            d.scope().thread_id() == &thread.id
                && d.scope().thread_generation() == &thread.generation
                && thread.origin.as_ref() == Some(d.scope().origin()),
        )?;
        execution(executions, d.creator_execution_id())?;
        let references = h
            .acceptances
            .iter()
            .flat_map(|a| a.decisions())
            .filter(|a| a.permission_id() == Some(d.permission_id()))
            .count();
        check(references == 1)?;
    }
    for u in &h.uses {
        let e = execution(executions, u.execution_id())?;
        retained_before(u.before_snapshot(), e, &h.journal)?;
        let d = h
            .decisions
            .iter()
            .find(|d| d.permission_id() == u.permission_id())
            .ok_or_else(invalid)?;
        let s = d.scope();
        let committed = point(&h.journal, u.execution_id(), u.committed_revision())?;
        let task = u
            .before_snapshot()
            .tasks
            .iter()
            .find(|t| &t.spec.id == u.task_id())
            .ok_or_else(invalid)?;
        check(
            u.thread_id() == &thread.id
                && u.thread_generation() == &thread.generation
                && s.definition() == &u.before_snapshot().definition
                && s.effect_id() == &task.spec.effect_id
                && s.application() == u.application()
                && s.tool_id() == u.tool_id()
                && s.tool_revision() == u.tool_revision()
                && s.policy_revision() == u.policy_revision()
                && s.constraint_revision() == u.constraint_revision()
                && committed.owner_generation == u.before_snapshot().owner_generation
                && committed.updated_at_ms == u.facts().now_ms
                && e.snapshot
                    .turn_history
                    .contains(&u.before_snapshot().turn_id),
        )?;
        let retained = e
            .snapshot
            .tasks
            .iter()
            .find(|t| &t.spec.id == u.task_id())
            .ok_or_else(invalid)?;
        check(
            retained.spec == task.spec
                && retained.input == task.input
                && retained.approved
                && retained.interrupt.as_ref() == Some(u.marker()),
        )?;
    }
    // Every consumed grant has exactly one original acceptance of either kind.
    for g in &h.grants {
        let ordinary = h
            .ordinary
            .iter()
            .filter(|a| a.execution_id == g.execution_id)
            .flat_map(|a| &a.decisions)
            .filter(|d| d.interrupt_id == g.interrupt_id)
            .count();
        let permission = h
            .acceptances
            .iter()
            .flat_map(|a| a.decisions())
            .filter(|d| {
                d.grant_ref().execution_id() == &g.execution_id
                    && d.grant_ref().interrupt_id() == &g.interrupt_id
            })
            .count();
        check(if g.consumed {
            ordinary + permission == 1
        } else {
            ordinary + permission == 0
        })?;
    }
    Ok(())
}

fn capture_in(
    store: &SqliteStore,
    connection: &Connection,
    target: &ThreadDeletionTarget,
    account: &mut Account,
) -> Result<Capture, PermissionError> {
    let id = target.thread_id.as_str();
    let mut source = Vec::new();
    metadata(connection, store.owner_generation, &mut source, account)?;
    reverse_relations(connection, id)?;
    let threads = read_rows(connection,
        "SELECT id,active_execution_id,created_at_ms,metadata,origin,resource_revision,generation FROM threads WHERE id=?1 LIMIT 2",
        id, 1, &[256,256,8,262144,262144,8,64], &mut source, account)?;
    if threads.len() != 1 {
        return Err(StoreError::RevisionConflict.into());
    }
    let r = &threads[0];
    check(string(&r[0])? == id)?;
    let thread = account.thread(PermissionThreadCells {
        id: &target.thread_id,
        active_execution_id: optional(&r[1])?,
        created_at_ms: blobcell(&r[2])?,
        metadata: bytes(&r[3])?,
        origin: optional(&r[4])?,
        resource_revision: blobcell(&r[5])?,
        generation: bytes(&r[6])?,
    })?;
    let rows = read_rows(connection,
        "SELECT id,thread_id,revision,owner_epoch,snapshot FROM executions WHERE thread_id=?1 ORDER BY id LIMIT 17",
        id, 16, &[256,256,8,8,1048576], &mut source, account)?;
    let mut executions = Vec::new();
    for r in &rows {
        let execution_id = ExecutionId::new(string(&r[0])?).map_err(|_| invalid())?;
        let e = account.execution(PermissionExecutionCells {
            execution_id: &execution_id,
            thread_id: bytes(&r[1])?,
            revision: blobcell(&r[2])?,
            owner_generation: blobcell(&r[3])?,
            snapshot: bytes(&r[4])?,
        })?;
        check(e.thread_id == thread.id)?;
        executions.push(e);
    }
    // Historical owner is intentionally permitted; only the metadata owner is
    // required to match the currently opened Store's writer generation.
    account.charge(
        rows.iter()
            .map(|r| match &r[4] {
                Cell::Text(v) => v.len(),
                _ => 0,
            })
            .sum(),
        1,
    )?;
    let active = thread
        .active_execution_id
        .as_ref()
        .map(|id| execution(&executions, id).cloned())
        .transpose()?;
    let view = ThreadView {
        thread,
        execution: active,
    };
    let thread = &view.thread;
    b4_runtime::validate_thread_deletion(&view, target, &executions)?;
    check(executions.iter().all(|e| {
        matches!(
            e.snapshot.status,
            b4_contract::ExecutionStatus::Completed
                | b4_contract::ExecutionStatus::Failed
                | b4_contract::ExecutionStatus::Cancelled
        )
    }))?;
    let mut h = History {
        grants: Vec::new(),
        ordinary: Vec::new(),
        acceptances: Vec::new(),
        decisions: Vec::new(),
        uses: Vec::new(),
        journal: Vec::new(),
    };
    let journal = read_rows(connection,
        "SELECT sequence,kind,thread_id,execution_id,revision,owner_epoch,status,updated_at_ms,payload FROM journal WHERE thread_id=?1 ORDER BY sequence LIMIT 513",
        id, 512, &[8,16,256,256,8,8,64,8,65536], &mut source, account)?;
    let mut creation = 0;
    for r in &journal {
        check(number(&r[0])? > 0 && string(&r[2])? == id)?;
        let owner = u64cell(&r[5])?;
        let time = u64cell(&r[7])?;
        check(owner > 0 && owner <= store.owner_generation)?;
        match string(&r[1])? {
            "thread" => {
                check(
                    matches!(
                        (&r[3], &r[4], &r[6], &r[8]),
                        (Cell::Null, Cell::Null, Cell::Null, Cell::Null)
                    ) && time == thread.created_at_ms,
                )?;
                creation += 1;
            }
            kind @ ("admit" | "commit") => {
                let e_id = ExecutionId::new(string(&r[3])?).map_err(|_| invalid())?;
                let e = execution(&executions, &e_id)?;
                let revision = u64cell(&r[4])?;
                let entry = account.journal(bytes(&r[8])?)?;
                let status: b4_contract::ExecutionStatus =
                    serde_json::from_slice(bytes(&r[6])?).map_err(|_| invalid())?;
                check(
                    entry.execution_id == e_id
                        && entry.revision == revision
                        && entry.owner_generation == owner
                        && entry.updated_at_ms == time
                        && entry.status == status
                        && revision <= e.snapshot.revision
                        && owner <= e.snapshot.owner_generation
                        && time <= e.snapshot.updated_at_ms
                        && ((kind == "admit" && revision == 0)
                            || (kind == "commit" && revision != 0)),
                )?;
                h.journal.push(entry);
            }
            _ => return Err(invalid()),
        }
    }
    check(creation == 1)?;
    let grants = read_rows(connection,
        "SELECT execution_id,interrupt_id,task_id,thread_id,thread_generation,task_parking_revision,consumed,record FROM approval_grants WHERE thread_id=?1 ORDER BY execution_id,interrupt_id LIMIT 65",
        id, 64, &[256,256,256,256,64,8,8,65536], &mut source, account)?;
    for r in &grants {
        let g = account.grant(bytes(&r[7])?)?;
        check(
            g.execution_id.as_str() == string(&r[0])?
                && g.interrupt_id.as_str() == string(&r[1])?
                && g.task_id.as_str() == string(&r[2])?
                && g.thread_id == thread.id
                && string(&r[3])? == id
                && g.thread_generation == thread.generation
                && g.thread_generation.as_str() == string(&r[4])?
                && g.task_parking_revision == u64cell(&r[5])?
                && number(&r[6])? == i64::from(g.consumed),
        )?;
        h.grants.push(g);
    }
    let ordinary = read_rows(connection,
        "SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,definition_revision,accepted_at_ms,record FROM approval_acceptances WHERE thread_id=?1 ORDER BY execution_id,turn_id LIMIT 65",
        id, 64, &[256,256,256,64,8,256,8,65536], &mut source, account)?;
    for r in &ordinary {
        let a = account.ordinary_acceptance(bytes(&r[7])?)?;
        check(
            a.execution_id.as_str() == string(&r[0])?
                && a.turn_id.as_str() == string(&r[1])?
                && a.thread_id == thread.id
                && string(&r[2])? == id
                && a.thread_generation == thread.generation
                && a.thread_generation.as_str() == string(&r[3])?
                && a.previous_revision == u64cell(&r[4])?
                && a.definition_revision.as_str() == string(&r[5])?
                && a.accepted_at_ms == u64cell(&r[6])?,
        )?;
        h.ordinary.push(a);
    }
    let acceptances = read_rows(connection,
        "SELECT execution_id,turn_id,thread_id,thread_generation,previous_revision,committed_revision,record_version,record FROM permission_acceptances WHERE thread_id=?1 ORDER BY execution_id,turn_id LIMIT 65",
        id, 64, &[256,256,256,64,8,8,8,65536], &mut source, account)?;
    for r in &acceptances {
        let a = account.acceptance(bytes(&r[7])?)?;
        check(
            a.before_snapshot().execution_id.as_str() == string(&r[0])?
                && a.turn_id().as_str() == string(&r[1])?
                && a.thread_id() == &thread.id
                && string(&r[2])? == id
                && a.thread_generation() == &thread.generation
                && a.thread_generation().as_str() == string(&r[3])?
                && a.before_snapshot().revision == u64cell(&r[4])?
                && a.committed_revision() == u64cell(&r[5])?
                && i64::from(a.record_version()) == number(&r[6])?,
        )?;
        h.acceptances.push(a);
    }
    let decisions = read_rows(connection,
        "SELECT permission_id,thread_id,thread_generation,scope_digest,record_version,record FROM permission_decisions WHERE thread_id=?1 ORDER BY permission_id LIMIT 65",
        id, 64, &[256,256,64,64,8,65536], &mut source, account)?;
    for r in &decisions {
        let d = account.decision(bytes(&r[5])?)?;
        check(
            d.permission_id().as_str() == string(&r[0])?
                && d.scope().thread_id() == &thread.id
                && string(&r[1])? == id
                && d.scope().thread_generation() == &thread.generation
                && d.scope().thread_generation().as_str() == string(&r[2])?
                && d.scope_digest() == string(&r[3])?
                && i64::from(d.record_version()) == number(&r[4])?,
        )?;
        h.decisions.push(d);
    }
    let uses = read_rows(connection,
        "SELECT execution_id,task_id,use_id,thread_id,thread_generation,permission_id,turn_id,authorization_revision,record_version,record FROM permission_uses WHERE thread_id=?1 ORDER BY execution_id,task_id LIMIT 65",
        id, 64, &[256,256,256,256,64,256,256,8,8,16384], &mut source, account)?;
    for r in &uses {
        let u = account.permission_use(bytes(&r[9])?)?;
        check(
            u.execution_id().as_str() == string(&r[0])?
                && u.task_id().as_str() == string(&r[1])?
                && u.use_id().as_str() == string(&r[2])?
                && u.thread_id() == &thread.id
                && string(&r[3])? == id
                && u.thread_generation() == &thread.generation
                && u.thread_generation().as_str() == string(&r[4])?
                && u.permission_id().as_str() == string(&r[5])?
                && u.before_snapshot().turn_id.as_str() == string(&r[6])?
                && u.committed_revision() == u64cell(&r[7])?
                && i64::from(u.record_version()) == number(&r[8])?,
        )?;
        h.uses.push(u);
    }
    validate_history(thread, &executions, &h, account)?;
    let counts = [
        1,
        executions.len(),
        journal.len(),
        grants.len(),
        ordinary.len(),
        acceptances.len(),
        decisions.len(),
        uses.len(),
    ];
    Capture::from_validated_source(
        view,
        executions,
        store.owner_generation,
        source,
        counts,
        target,
        account,
    )
}

pub(super) fn capture(
    store: &SqliteStore,
    target: &ThreadDeletionTarget,
    account: &mut Account,
) -> Result<Capture, PermissionError> {
    let mut connection = store.connection()?;
    connection.with_code_only_errors(|connection: &Connection| {
        let tx = Transaction::new_unchecked(connection, TransactionBehavior::Deferred)
            .map_err(storage)?;
        let capture = capture_in(store, &tx, target, account)?;
        tx.commit().map_err(storage)?;
        Ok(capture)
    })
}

pub(super) fn commit(
    store: &SqliteStore,
    capture: &Capture,
    target: &ThreadDeletionTarget,
    account: &mut Account,
) -> Result<Commit, PermissionError> {
    let mut connection = store.connection()?;
    connection.with_code_only_errors(|connection: &Connection| {
        let tx = Transaction::new_unchecked(connection, TransactionBehavior::Immediate).map_err(storage)?;
        let current = capture_in(store, &tx, target, account)?;
        if !capture.matches(&current, account)? { return Err(StoreError::RevisionConflict.into()); }
        account.charge(8192, 32)?;
        let thread = &target.thread_id;
        let updated = tx.execute("UPDATE threads SET active_execution_id=NULL WHERE id=?1 AND generation=?2 AND resource_revision=?3",
            params![thread.as_str(), target.thread_generation.as_str(), target.resource_revision.to_be_bytes().as_slice()]).map_err(storage)?;
        check(updated == 1)?;
        let mut removed = [0;8];
        // Dependencies first, with exact original physical counts checked even
        // for tables which are empty. Transaction drop rolls back all failures.
        for (index, table) in [(7,"permission_uses"), (6,"permission_decisions"), (5,"permission_acceptances"),
            (4,"approval_acceptances"), (3,"approval_grants"), (2,"journal"), (1,"executions")] {
            removed[index] = tx.execute(&format!("DELETE FROM {table} WHERE thread_id=?1"), [thread.as_str()]).map_err(storage)?;
            check(removed[index] == capture.counts()[index])?;
            check(!exists(&tx, &format!("SELECT EXISTS(SELECT 1 FROM {table} WHERE thread_id=?1)"), thread.as_str())?)?;
        }
        removed[0] = tx.execute("DELETE FROM threads WHERE id=?1 AND generation=?2 AND resource_revision=?3",
            params![thread.as_str(), target.thread_generation.as_str(), target.resource_revision.to_be_bytes().as_slice()])
            .map_err(|_| PermissionError::Store(StoreError::Unavailable("permission_settled_thread_deletion_final_thread_delete_failed".into())))?;
        check(removed[0] == 1)?;
        check(!exists(&tx, "SELECT EXISTS(SELECT 1 FROM threads WHERE id=?1)", thread.as_str())?)?;
        let committed = capture.acknowledge_removed(removed)?;
        tx.commit().map_err(|_| PermissionError::Store(StoreError::Unavailable("permission_settled_thread_deletion_commit_failed".into())))?;
        Ok(committed)
    })
}
