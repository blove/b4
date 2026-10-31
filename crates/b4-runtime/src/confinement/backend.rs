use super::{
    shape, RecoveryConfinementBackendError as Error, RecoveryConfinementReason,
    RecoveryConfinementReceipt, RecoveryConfinementTarget,
};
use crate::approval::{ApprovalAcceptance, GrantRecord};
use crate::{JournalEntry, ThreadOrigin, ThreadRecord, ThreadView};
use b4_contract::{Snapshot, TransitionFacts, TurnId};
use serde::de::{DeserializeOwned, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::fmt;

pub const SNAPSHOT_BYTES: usize = 1_048_576;
pub const THREAD_BYTES: usize = 262_144;
pub const AUTHORITY_BYTES: usize = 131_072;
pub const JOURNAL_BYTES: usize = 4_096;
pub const AUTHORITY_ROWS: usize = 64;
pub const JOURNAL_ROWS: usize = 4_096;
pub const AGGREGATE_BYTES: usize = 8_388_608;
pub const AGGREGATE_NODES: usize = 65_536;

/// Input-only accounting. Raw SQL and borrowed canonical Memory input are
/// distinct representations; replacement encoding has its own bounds.
#[derive(Default)]
pub struct AuditBudget {
    bytes: usize,
    nodes: usize,
}
impl AuditBudget {
    pub fn check_bytes(&self, bytes: usize) -> Result<(), Error> {
        self.bytes
            .checked_add(bytes)
            .filter(|n| *n <= AGGREGATE_BYTES)
            .map(|_| ())
            .ok_or(Error::LimitExceeded)
    }
    pub fn add_bytes(&mut self, bytes: usize) -> Result<(), Error> {
        let next = self
            .bytes
            .checked_add(bytes)
            .filter(|n| *n <= AGGREGATE_BYTES)
            .ok_or(Error::LimitExceeded)?;
        self.bytes = next;
        Ok(())
    }
    pub fn add(&mut self, stats: shape::ShapeStats) -> Result<(), Error> {
        let bytes = self
            .bytes
            .checked_add(stats.bytes)
            .filter(|n| *n <= AGGREGATE_BYTES)
            .ok_or(Error::LimitExceeded)?;
        let nodes = self
            .nodes
            .checked_add(stats.nodes)
            .filter(|n| *n <= AGGREGATE_NODES)
            .ok_or(Error::LimitExceeded)?;
        self.bytes = bytes;
        self.nodes = nodes;
        Ok(())
    }
    pub fn raw(&mut self, bytes: &[u8], limit: usize) -> Result<(), Error> {
        self.add(shape::check_raw(bytes, limit)?)
    }
    pub fn borrowed<T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
        limit: usize,
    ) -> Result<(), Error> {
        self.add(shape::measure(value, limit)?)
    }
}

/// O(1) chain state plus a fixed authority-derived set of revision witnesses.
/// It never retains the complete streamed journal. Each input entry must have
/// passed its representation-specific byte/shape/index accounting first.
pub struct JournalAudit<'a> {
    snapshot: &'a Snapshot,
    count: usize,
    previous: Option<JournalEntry>,
    first_owner: u64,
    wanted: Vec<u64>,
    witnesses: Vec<JournalEntry>,
}
pub struct JournalEvidence {
    count: usize,
    first_owner: u64,
    last: JournalEntry,
    witnesses: Vec<JournalEntry>,
}
impl<'a> JournalAudit<'a> {
    pub fn new(
        snapshot: &'a Snapshot,
        grants: &[GrantRecord],
        acceptances: &[ApprovalAcceptance],
    ) -> Result<Self, Error> {
        if grants.len() > AUTHORITY_ROWS || acceptances.len() > AUTHORITY_ROWS {
            return Err(Error::LimitExceeded);
        }
        let capacity = grants.len() + acceptances.len() * 2;
        let mut wanted = Vec::new();
        wanted
            .try_reserve_exact(capacity)
            .map_err(|_| Error::Unavailable)?;
        for revision in grants.iter().map(|g| g.task_parking_revision) {
            if !wanted.contains(&revision) {
                wanted.push(revision);
            }
        }
        for acceptance in acceptances {
            let next = acceptance
                .previous_revision
                .checked_add(1)
                .ok_or(Error::CorruptRecord)?;
            for revision in [acceptance.previous_revision, next] {
                if !wanted.contains(&revision) {
                    wanted.push(revision);
                }
            }
        }
        let mut witnesses = Vec::new();
        witnesses
            .try_reserve_exact(wanted.len())
            .map_err(|_| Error::Unavailable)?;
        Ok(Self {
            snapshot,
            count: 0,
            previous: None,
            first_owner: 0,
            wanted,
            witnesses,
        })
    }
    pub fn observe(&mut self, entry: JournalEntry) -> Result<(), Error> {
        let maximum = JOURNAL_ROWS
            - usize::from(self.snapshot.status != b4_contract::ExecutionStatus::RecoveryBlocked);
        if self.count == maximum {
            return Err(Error::LimitExceeded);
        }
        if entry.execution_id != self.snapshot.execution_id
            || entry.owner_generation == 0
            || entry.owner_generation > self.snapshot.owner_generation
            || entry.revision > self.snapshot.revision
            || entry.updated_at_ms > self.snapshot.updated_at_ms
        {
            return Err(Error::CorruptRecord);
        }
        if let Some(previous) = &self.previous {
            if previous.revision.checked_add(1) != Some(entry.revision)
                || previous.owner_generation > entry.owner_generation
                || previous.updated_at_ms > entry.updated_at_ms
            {
                return Err(Error::CorruptRecord);
            }
        } else {
            if entry.revision != 0 || entry.status != b4_contract::ExecutionStatus::Accepted {
                return Err(Error::CorruptRecord);
            }
            self.first_owner = entry.owner_generation;
        }
        if self.wanted.contains(&entry.revision) {
            self.witnesses.push(entry.clone());
        }
        self.previous = Some(entry);
        self.count += 1;
        Ok(())
    }
    pub fn finish(self) -> Result<JournalEvidence, Error> {
        let last = self.previous.ok_or(Error::CorruptRecord)?;
        let evidence = JournalEvidence {
            count: self.count,
            first_owner: self.first_owner,
            last,
            witnesses: self.witnesses,
        };
        if !evidence.matches(self.snapshot) {
            return Err(Error::CorruptRecord);
        }
        Ok(evidence)
    }
}
impl JournalEvidence {
    pub fn count(&self) -> usize {
        self.count
    }
    pub fn first_owner(&self) -> u64 {
        self.first_owner
    }
    pub fn revision(&self, revision: u64) -> Option<&JournalEntry> {
        self.witnesses.iter().find(|j| j.revision == revision)
    }
    pub fn matches(&self, snapshot: &Snapshot) -> bool {
        self.last.execution_id == snapshot.execution_id
            && self.last.revision == snapshot.revision
            && self.last.owner_generation == snapshot.owner_generation
            && self.last.status == snapshot.status
            && self.last.updated_at_ms == snapshot.updated_at_ms
    }
}

/// Guard actual persisted bytes before version probing or recursive decoding.
/// No canonical Snapshot input cap is applied to the decoded SQL value.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, Error> {
    shape::check_raw(bytes, SNAPSHOT_BYTES)?;
    let version: VersionProbe = serde_json::from_slice(bytes).map_err(|_| Error::CorruptRecord)?;
    if version.0 != u64::from(b4_contract::SNAPSHOT_VERSION) {
        return Err(Error::UnsupportedSnapshot);
    }
    serde_json::from_slice(bytes).map_err(|_| Error::CorruptRecord)
}

pub fn decode_record<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, Error> {
    shape::check_raw(bytes, limit)?;
    serde_json::from_slice(bytes).map_err(|_| Error::CorruptRecord)
}

struct VersionProbe(u64);
impl<'de> Deserialize<'de> for VersionProbe {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Probe;
        impl<'de> Visitor<'de> for Probe {
            type Value = VersionProbe;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("snapshot object")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let mut version = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "schema_version" {
                        if version.is_some() {
                            return Err(serde::de::Error::duplicate_field("schema_version"));
                        }
                        version = Some(map.next_value::<u64>()?);
                    } else {
                        let _ = map.next_value::<IgnoredAny>()?;
                    }
                }
                version
                    .map(VersionProbe)
                    .ok_or_else(|| serde::de::Error::missing_field("schema_version"))
            }
        }
        deserializer.deserialize_map(Probe)
    }
}

pub fn measure_thread(thread: &ThreadRecord, budget: &mut AuditBudget) -> Result<(), Error> {
    let metadata = shape::measure(&thread.metadata, THREAD_BYTES)?;
    let origin = thread
        .origin
        .as_ref()
        .map(|v| shape::measure(v, THREAD_BYTES))
        .transpose()?;
    let scalar_bytes = thread
        .id
        .as_str()
        .len()
        .checked_add(thread.generation.as_str().len())
        .and_then(|n| {
            n.checked_add(
                thread
                    .active_execution_id
                    .as_ref()
                    .map_or(0, |v| v.as_str().len()),
            )
        })
        .and_then(|n| n.checked_add(16))
        .ok_or(Error::LimitExceeded)?;
    let total = scalar_bytes
        .checked_add(metadata.bytes)
        .and_then(|n| n.checked_add(origin.map_or(0, |v| v.bytes)))
        .filter(|n| *n <= THREAD_BYTES)
        .ok_or(Error::LimitExceeded)?;
    let _ = total;
    budget.add_bytes(scalar_bytes)?;
    budget.add(metadata)?;
    if let Some(origin) = origin {
        budget.add(origin)?;
    }
    Ok(())
}

pub fn validate_origin(origin: Option<&ThreadOrigin>) -> Result<(), Error> {
    if origin.is_some_and(|o| {
        [&o.application, &o.tenant, &o.principal]
            .iter()
            .any(|s| s.is_empty() || s.len() > 256 || s.chars().any(char::is_control))
    }) {
        return Err(Error::CorruptRecord);
    }
    Ok(())
}

pub fn decode_metadata(bytes: &[u8]) -> Result<Map<String, Value>, Error> {
    decode_record(bytes, THREAD_BYTES)
}

/// Caller has performed its representation-specific preflight and full audit.
/// The pure kernel's 8MiB safe-clone guard is not a second SQL 1MiB read cap.
pub fn prepare_change(
    original: &ThreadView,
    target: &RecoveryConfinementTarget,
    reason: RecoveryConfinementReason,
    fresh: &TurnId,
    owner: u64,
    facts: TransitionFacts,
) -> Result<(Snapshot, RecoveryConfinementReceipt), Error> {
    let current = &original
        .execution
        .as_ref()
        .ok_or(Error::NotActive)?
        .snapshot;
    if current.turn_history.contains(fresh) {
        return Err(Error::InvalidTurn);
    }
    if facts.now_ms < current.updated_at_ms {
        return Err(Error::ClockRegression);
    }
    let resource = original
        .thread
        .resource_revision
        .checked_add(1)
        .ok_or(Error::Unavailable)?;
    let plan =
        b4_kernel::propose_recovery_confinement(current, reason, fresh.clone(), owner, facts)
            .map_err(|error| match error {
                b4_kernel::KernelError::ClockMovedBackwards => Error::ClockRegression,
                b4_kernel::KernelError::StaleTurn => Error::InvalidTurn,
                b4_kernel::KernelError::RevisionOverflow => Error::Unavailable,
                b4_kernel::KernelError::InvalidTransition(
                    "recovery_confinement_limit_exceeded",
                ) => Error::LimitExceeded,
                b4_kernel::KernelError::InvalidTransition(
                    "recovery_confinement_encoding_failed",
                ) => Error::Unavailable,
                _ => Error::CorruptRecord,
            })?;
    if !plan.dispatchable.is_empty() {
        return Err(Error::CorruptRecord);
    }
    shape::measure(&plan.snapshot, SNAPSHOT_BYTES)?;
    shape::measure(&JournalEntry::from(&plan.snapshot), JOURNAL_BYTES)?;
    let receipt = RecoveryConfinementReceipt {
        thread_id: original.thread.id.clone(),
        thread_generation: original.thread.generation.clone(),
        execution_id: current.execution_id.clone(),
        original_fence: target.execution_fence,
        new_fence: (&plan.snapshot).into(),
        original_resource_revision: original.thread.resource_revision,
        new_resource_revision: resource,
        original_turn_id: current.turn_id.clone(),
        fresh_turn_id: fresh.clone(),
        route: current.definition.route.clone(),
        definition_revision: current.definition.revision.clone(),
        reason,
    };
    Ok((plan.snapshot, receipt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use b4_contract::*;
    use serde_json::{Map, Value};

    fn initial() -> Snapshot {
        b4_kernel::new_execution(
            ExecutionId::new("helper-execution").unwrap(),
            TurnId::new("helper-turn").unwrap(),
            1,
            Definition {
                route: RouteKey::new("/helper#workflow").unwrap(),
                revision: ExecutionRevision::new("helper-v1").unwrap(),
                batches: vec![vec![TaskSpec {
                    id: TaskId::new("helper-task").unwrap(),
                    effect_id: EffectId::new("helper-effect").unwrap(),
                    recovery: RecoveryPolicy::Pure,
                    requires_approval: false,
                }]],
                max_attempts: 3,
            },
            Map::new(),
            TransitionFacts { now_ms: 1 },
        )
        .unwrap()
    }
    fn chain() -> (Snapshot, Vec<JournalEntry>) {
        let original = initial();
        let started = b4_kernel::transition(
            &original,
            KernelCommand::Start,
            TransitionFacts { now_ms: 2 },
        )
        .unwrap()
        .snapshot;
        let recovered = b4_kernel::transition(
            &started,
            KernelCommand::Recover {
                available_revision: started.definition.revision.clone(),
                turn_id: TurnId::new("helper-recovered").unwrap(),
                owner_generation: 2,
            },
            TransitionFacts { now_ms: 3 },
        )
        .unwrap()
        .snapshot;
        b4_kernel::validate_snapshot(&recovered).unwrap();
        (
            recovered.clone(),
            [&original, &started, &recovered]
                .map(JournalEntry::from)
                .to_vec(),
        )
    }

    #[test]
    fn aggregate_byte_limit_is_checked_without_partial_accounting() {
        let mut budget = AuditBudget::default();
        budget.add_bytes(AGGREGATE_BYTES).unwrap();
        assert_eq!(budget.add_bytes(1), Err(Error::LimitExceeded));
        assert_eq!(budget.bytes, AGGREGATE_BYTES);
        budget.add_bytes(0).unwrap();
    }
    #[test]
    fn aggregate_node_limit_is_checked_atomically_with_bytes() {
        let mut budget = AuditBudget::default();
        budget
            .add(shape::ShapeStats {
                bytes: 7,
                nodes: AGGREGATE_NODES,
            })
            .unwrap();
        assert_eq!(
            budget.add(shape::ShapeStats { bytes: 9, nodes: 1 }),
            Err(Error::LimitExceeded)
        );
        assert_eq!((budget.bytes, budget.nodes), (7, AGGREGATE_NODES));
    }
    #[test]
    fn snapshot_version_probe_distinguishes_unsigned_future_from_malformed() {
        assert!(matches!(
            decode_snapshot(br#"{"schema_version":18446744073709551615}"#),
            Err(Error::UnsupportedSnapshot)
        ));
        for bytes in [
            br#"{"schema_version":-1}"#.as_slice(),
            br#"{"schema_version":1,"schema_version":1}"#,
            br#"{"schema_version":1.0}"#,
            br#"{"other":1}"#,
            br#"{"schema_version":1} garbage"#,
        ] {
            assert!(matches!(decode_snapshot(bytes), Err(Error::CorruptRecord)));
        }
    }
    #[test]
    fn raw_snapshot_decoder_does_not_apply_a_canonical_input_cap() {
        let mut snapshot = initial();
        snapshot.values.insert("number".into(), Value::from(100.0));
        snapshot
            .values
            .insert("padding".into(), Value::String(String::new()));
        let overhead = serde_json::to_vec(&snapshot).unwrap().len();
        snapshot.values.insert(
            "padding".into(),
            Value::String("x".repeat(SNAPSHOT_BYTES + 1 - overhead)),
        );
        let canonical = serde_json::to_string(&snapshot).unwrap();
        assert_eq!(canonical.len(), SNAPSHOT_BYTES + 1);
        let raw = canonical.replace("\"number\":100.0", "\"number\":1e2");
        assert_eq!(raw.len(), SNAPSHOT_BYTES - 1);
        b4_kernel::validate_snapshot(&snapshot).unwrap();
        assert_eq!(decode_snapshot(raw.as_bytes()).unwrap(), snapshot);
    }
    #[test]
    fn streamed_source_generated_chain_checks_the_complete_final_projection() {
        let (snapshot, entries) = chain();
        let mut audit = JournalAudit::new(&snapshot, &[], &[]).unwrap();
        for entry in entries {
            audit.observe(entry).unwrap();
        }
        let evidence = audit.finish().unwrap();
        assert_eq!(evidence.count(), 3);
        assert_eq!(evidence.first_owner(), 1);
        assert!(evidence.matches(&snapshot));
        assert!(evidence.witnesses.is_empty());
    }
    #[test]
    fn streamed_chain_refuses_gaps_owner_time_and_identity_contradictions() {
        let (snapshot, entries) = chain();
        for kind in 0..4 {
            let mut audit = JournalAudit::new(&snapshot, &[], &[]).unwrap();
            audit.observe(entries[0].clone()).unwrap();
            let mut bad = entries[1].clone();
            match kind {
                0 => bad.revision += 1,
                1 => bad.owner_generation = 0,
                2 => bad.updated_at_ms = 0,
                _ => bad.execution_id = ExecutionId::new("other-execution").unwrap(),
            }
            assert_eq!(audit.observe(bad), Err(Error::CorruptRecord));
        }
    }
    #[test]
    fn streamed_chain_refuses_missing_or_inexact_final_record() {
        let (snapshot, entries) = chain();
        assert!(matches!(
            JournalAudit::new(&snapshot, &[], &[]).unwrap().finish(),
            Err(Error::CorruptRecord)
        ));
        let mut audit = JournalAudit::new(&snapshot, &[], &[]).unwrap();
        audit.observe(entries[0].clone()).unwrap();
        audit.observe(entries[1].clone()).unwrap();
        assert!(matches!(audit.finish(), Err(Error::CorruptRecord)));
        let mut audit = JournalAudit::new(&snapshot, &[], &[]).unwrap();
        for mut entry in entries {
            if entry.revision == snapshot.revision {
                entry.status = ExecutionStatus::Accepted;
            }
            audit.observe(entry).unwrap();
        }
        assert!(matches!(audit.finish(), Err(Error::CorruptRecord)));
    }
    #[test]
    fn journal_count_reserves_one_changed_row_but_none_for_already_blocked() {
        // Synthetic JournalEntry projections exercise only the streaming count
        // boundary; they are not claimed as a persisted kernel history fixture.
        for (status, maximum) in [
            (ExecutionStatus::Running, JOURNAL_ROWS - 1),
            (ExecutionStatus::RecoveryBlocked, JOURNAL_ROWS),
        ] {
            let mut snapshot = initial();
            snapshot.status = status;
            snapshot.revision = maximum as u64 - 1;
            snapshot.updated_at_ms = maximum as u64;
            let mut audit = JournalAudit::new(&snapshot, &[], &[]).unwrap();
            for revision in 0..maximum as u64 {
                audit
                    .observe(JournalEntry {
                        execution_id: snapshot.execution_id.clone(),
                        revision,
                        owner_generation: 1,
                        status: if revision == 0 {
                            ExecutionStatus::Accepted
                        } else {
                            status
                        },
                        updated_at_ms: revision + 1,
                    })
                    .unwrap();
            }
            assert_eq!(
                audit.observe(JournalEntry::from(&snapshot)),
                Err(Error::LimitExceeded)
            );
            assert_eq!(audit.finish().unwrap().count(), maximum);
        }
    }
}
