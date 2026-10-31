use b4_contract::Snapshot;
use b4_http::{StartupPreparationPhase, StartupPreparationStatus, StartupProgress};
use b4_runtime::{
    approval::{ApprovalAcceptance, GrantRecord},
    JournalEntry, StoredExecution, ThreadRecord, ThreadView,
};
use serde::{Deserialize, Serialize};

/// Public diagnostics carry only this closed, static fixture taxonomy.
#[derive(Debug, Clone, Copy)]
pub enum Failure {
    Setup,
    Io,
    Permission,
    Bound,
    Protocol,
    Timeout,
    Child,
    Cleanup,
    Semantics,
}
pub type Result<T> = std::result::Result<T, Failure>;
pub fn require(ok: bool) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Failure::Semantics)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lane {
    Approval,
    Unsafe,
}
impl Lane {
    pub fn text(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::Unsafe => "unsafe",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "approval" => Ok(Self::Approval),
            "unsafe" => Ok(Self::Unsafe),
            _ => Err(Failure::Protocol),
        }
    }
    pub fn route(self) -> &'static str {
        match self {
            Self::Approval => "/restart-approval#workflow",
            Self::Unsafe => "/restart-unsafe#workflow",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Seed,
    Restart,
}
impl Role {
    pub fn text(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::Restart => "restart",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "seed" => Ok(Self::Seed),
            "restart" => Ok(Self::Restart),
            _ => Err(Failure::Protocol),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    NotStarted,
    Preparing,
    Complete,
    Incomplete,
}
impl From<StartupPreparationPhase> for Phase {
    fn from(s: StartupPreparationPhase) -> Self {
        match s {
            StartupPreparationPhase::NotStarted => Self::NotStarted,
            StartupPreparationPhase::Preparing => Self::Preparing,
            StartupPreparationPhase::Complete => Self::Complete,
            StartupPreparationPhase::Incomplete => Self::Incomplete,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Progress {
    pub pages: usize,
    pub scanned_rows: usize,
    pub candidates: usize,
    pub admitted: usize,
    pub runnable: usize,
    pub parked: usize,
    pub blocked: usize,
    pub terminal: usize,
}
impl From<StartupProgress> for Progress {
    fn from(p: StartupProgress) -> Self {
        Self {
            pages: p.pages,
            scanned_rows: p.scanned_rows,
            candidates: p.candidates,
            admitted: p.admitted,
            runnable: p.runnable,
            parked: p.parked,
            blocked: p.blocked,
            terminal: p.terminal,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Command {
    Checkpoint {},
    Begin {},
    ReleaseResolver {},
    ReleaseCallback {},
    AwaitComplete {},
    Capture {},
    AwaitTerminal {},
    Stop {},
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u8,
    pub ordinal: u8,
    pub command: Command,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub version: u8,
    pub ordinal: u8,
    pub lane: Lane,
    pub role: Role,
    pub pid: u32,
    pub owner: u64,
    pub port: u16,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateFacts {
    pub entered: bool,
    pub explicit: bool,
    pub returned: bool,
    pub expired: bool,
    pub cleanup: bool,
}
impl GateFacts {
    pub fn positive(self) -> bool {
        self.entered && self.explicit && self.returned && !self.expired && !self.cleanup
    }
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reply {
    pub version: u8,
    pub ordinal: u8,
    pub phase: Phase,
    pub progress: Progress,
    pub gate: GateFacts,
    pub capture: Option<CaptureName>,
    pub retired: bool,
}
impl Reply {
    pub fn from_status(
        ordinal: u8,
        status: Option<StartupPreparationStatus>,
        gate: GateFacts,
        capture: Option<CaptureName>,
        retired: bool,
    ) -> Self {
        Self {
            version: 1,
            ordinal,
            phase: status.map_or(Phase::NotStarted, |s| s.phase.into()),
            progress: status.map_or(Progress::default(), |s| s.progress.into()),
            gate,
            capture,
            retired,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureName {
    Parked,
    Dispatched,
    Source,
    Adopted,
    Final,
}
impl CaptureName {
    pub fn file(self) -> &'static str {
        match self {
            Self::Parked => "parked.json",
            Self::Dispatched => "dispatched.json",
            Self::Source => "source.json",
            Self::Adopted => "adopted.json",
            Self::Final => "final.json",
        }
    }
}
#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Counts {
    pub sibling: usize,
    pub release: usize,
    pub unsafe_effect: usize,
    pub health: usize,
    pub validators: usize,
}
/// Complete sensitive authority stays in mode600 evidence, never in Debug/IPC.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Authority {
    pub thread: ThreadRecord,
    pub execution: StoredExecution,
    pub grants: Vec<GrantRecord>,
    pub acceptances: Vec<ApprovalAcceptance>,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capture {
    pub version: u8,
    pub lane: Lane,
    pub role: Role,
    pub owner: u64,
    pub ordinary: ThreadView,
    pub authority: Authority,
    pub journal: Vec<JournalEntry>,
    pub counts: Counts,
}
impl Capture {
    pub fn snapshot(&self) -> Result<&Snapshot> {
        self.ordinary
            .execution
            .as_ref()
            .map(|e| &e.snapshot)
            .ok_or(Failure::Setup)
    }
    pub fn same_durable(&self, other: &Self) -> bool {
        self.ordinary == other.ordinary
            && self.authority == other.authority
            && self.journal == other.journal
    }
    pub fn coherent(&self) -> bool {
        self.ordinary.thread == self.authority.thread
            && self.ordinary.execution.as_ref() == Some(&self.authority.execution)
            && self.authority.grants.len() <= 1
            && self.authority.acceptances.len() <= 1
            && self.journal.len() <= 64
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retirement {
    pub version: u8,
    pub owner: u64,
    pub quiescent: bool,
    pub faulted: bool,
    pub listener_finished: bool,
    pub weak_store_absent: bool,
    pub weak_application_absent: bool,
    pub gate: GateFacts,
    pub phase: Phase,
    pub progress: Progress,
}
impl Retirement {
    pub fn positive(&self) -> bool {
        self.version == 1
            && self.quiescent
            && !self.faulted
            && self.listener_finished
            && self.weak_store_absent
            && self.weak_application_absent
            && self.gate.positive()
            && self.phase == Phase::Complete
    }
}
