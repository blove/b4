use super::{
    application,
    files::{self, Evidence},
    gate::{GateId, Gates},
    Error, Result,
};
use b4_contract::ThreadId;
use b4_runtime::{approval::ApprovalStore, Store};
use b4_sqlite::SqliteStore;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    io::{Read, Write},
    sync::Arc,
};
pub const LINE: usize = 8192;
pub const COMMANDS: usize = 256;
#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
enum Case {
    #[serde(rename = "setup")]
    Setup,
    #[serde(rename = "C1")]
    C1,
    #[serde(rename = "C2")]
    C2,
    #[serde(rename = "C3")]
    C3,
    #[serde(rename = "C4")]
    C4,
    #[serde(rename = "W1")]
    W1,
    #[serde(rename = "W2")]
    W2,
    #[serde(rename = "W3")]
    W3,
    #[serde(rename = "W4")]
    W4,
    #[serde(rename = "retirement")]
    Retirement,
}
impl Case {
    fn order(self) -> u8 {
        match self {
            Self::Setup => 0,
            Self::C1 => 1,
            Self::C2 => 2,
            Self::C3 => 3,
            Self::C4 => 4,
            Self::W1 => 5,
            Self::W2 => 6,
            Self::W3 => 7,
            Self::W4 => 8,
            Self::Retirement => 9,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arm {
    gate: GateId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GateRequest {
    gate: GateId,
    generation: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ThreadRequest {
    thread_id: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Command {
    Ready(Empty),
    Arm(Arm),
    AwaitEntered(GateRequest),
    InspectThread(ThreadRequest),
    InspectPending(ThreadRequest),
    GateStatus(GateRequest),
    Release(GateRequest),
    FinalStop(Empty),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    ordinal: usize,
    case: Case,
    command: Command,
}
fn decode(line: &[u8]) -> Result<Envelope> {
    if line.len() >= LINE {
        return Err(Error("control_line_too_large"));
    }
    serde_json::from_slice(line).map_err(|_| Error("control_invalid_request"))
}
fn read_line(reader: &mut impl Read) -> Result<Option<Vec<u8>>> {
    let mut bytes = Vec::with_capacity(LINE);
    let mut one = [0; 1];
    loop {
        match reader.read(&mut one) {
            Ok(0) => {
                return if bytes.is_empty() {
                    Ok(None)
                } else {
                    Err(Error("control_partial_line"))
                }
            }
            Ok(_) => {
                bytes.push(one[0]);
                if one[0] == b'\n' {
                    return Ok(Some(bytes));
                }
                if bytes.len() >= LINE {
                    return Err(Error("control_line_too_large"));
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(Error("control_read_failed")),
        }
    }
}
#[derive(serde::Serialize)]
pub struct Proof {
    pub commands: usize,
    pub final_stop: bool,
    pub pipe_eof: bool,
    pub observed_threads: Vec<String>,
}
struct Session {
    ordinal: usize,
    phase: u8,
    ready: bool,
    finished: bool,
    threads: BTreeSet<String>,
}
impl Session {
    fn new() -> Self {
        Self {
            ordinal: 0,
            phase: 0,
            ready: false,
            finished: false,
            threads: BTreeSet::new(),
        }
    }
    fn validate(&mut self, e: &Envelope) -> Result<()> {
        if e.version != 1
            || e.ordinal != self.ordinal + 1
            || e.ordinal > COMMANDS
            || self.finished
            || e.case.order() < self.phase
        {
            return Err(Error("control_invalid_phase"));
        }
        if !self.ready && !matches!(e.command, Command::Ready(_)) {
            return Err(Error("control_ready_required"));
        }
        if matches!(e.command, Command::Ready(_)) && (self.ready || e.case != Case::Setup) {
            return Err(Error("control_invalid_phase"));
        }
        if !matches!(e.command, Command::Ready(_)) && e.case == Case::Setup {
            return Err(Error("control_invalid_phase"));
        }
        let gate = match &e.command {
            Command::Arm(a) => Some(a.gate),
            Command::AwaitEntered(a) | Command::GateStatus(a) | Command::Release(a) => Some(a.gate),
            _ => None,
        };
        if gate.is_some_and(|g| match g {
            GateId::ControllerDispatch => e.case != Case::C4,
            GateId::WorkerHeld => !matches!(e.case, Case::W1 | Case::W3),
        }) {
            return Err(Error("control_gate_case"));
        }
        if (matches!(e.command, Command::FinalStop(_))) != (e.case == Case::Retirement) {
            return Err(Error("control_invalid_phase"));
        }
        self.ordinal = e.ordinal;
        self.phase = e.case.order();
        Ok(())
    }
    fn execute(
        &mut self,
        e: &Envelope,
        store: &SqliteStore,
        gates: &Gates,
        evidence: &Evidence,
        base: &str,
        token: &str,
    ) -> Result<Value> {
        self.validate(e)?;
        match &e.command {
            Command::Ready(_) => {
                self.ready = true;
                Ok(
                    json!({"base_url":base,"token":token,"routes":{"worker_value":application::WORKER_VALUE,"worker_void":application::WORKER_VOID,"worker_failure":application::WORKER_FAILURE,"worker_held":application::WORKER_HELD,"worker_park":application::WORKER_PARK}}),
                )
            }
            Command::Arm(a) => Ok(json!({"generation":gates.get(a.gate).arm()?})),
            Command::AwaitEntered(a) => Ok(serde_json::to_value(
                gates.get(a.gate).await_entered(a.generation)?,
            )
            .map_err(|_| Error("control_encoding_failed"))?),
            Command::GateStatus(a) => Ok(serde_json::to_value(
                gates.get(a.gate).status(a.generation)?,
            )
            .map_err(|_| Error("control_encoding_failed"))?),
            Command::Release(a) => Ok(serde_json::to_value(
                gates.get(a.gate).release(a.generation)?,
            )
            .map_err(|_| Error("control_encoding_failed"))?),
            Command::InspectThread(t) | Command::InspectPending(t) => {
                let id =
                    ThreadId::new(&t.thread_id).map_err(|_| Error("control_invalid_thread"))?;
                self.threads.insert(t.thread_id.clone());
                let view = store
                    .thread_view(&id)
                    .map_err(|_| Error("control_store_failed"))?;
                evidence.json(&format!("snapshot-{:04}-thread.json", e.ordinal), &view)?;
                let mut result = match &view {
                    None => {
                        json!({"found":false,"execution_status":null,"task_statuses":[],"revision":null,"owner_generation":null})
                    }
                    Some(view) => {
                        let current = view.execution.as_ref();
                        if current.is_some_and(|s| {
                            s.snapshot.revision > 9007199254740991
                                || s.snapshot.owner_generation > 9007199254740991
                        }) {
                            return Err(Error("control_integer_range"));
                        }
                        json!({"found":true,"execution_status":current.map(|s|s.snapshot.status),"task_statuses":current.map(|s|s.snapshot.tasks.iter().map(|t|t.status).collect::<Vec<_>>()).unwrap_or_default(),"revision":current.map(|s|s.snapshot.revision),"owner_generation":current.map(|s|s.snapshot.owner_generation)})
                    }
                };
                if matches!(e.command, Command::InspectPending(_)) {
                    let pending = match &view {
                        Some(view) => match &view.execution {
                            Some(execution) => store
                                .approval_view(&execution.snapshot.execution_id)
                                .map_err(|_| Error("control_store_failed"))?,
                            None => None,
                        },
                        None => None,
                    };
                    let snapshot=pending.as_ref().map(|p|json!({"thread":p.thread,"execution":p.execution,"grants":p.grants,"acceptances":p.acceptances}));
                    evidence.json(
                        &format!("snapshot-{:04}-approval.json", e.ordinal),
                        &snapshot,
                    )?;
                    let ids: Vec<_> = pending
                        .as_ref()
                        .map(|p| {
                            p.grants
                                .iter()
                                .filter(|g| !g.consumed)
                                .map(|g| g.interrupt_id.as_str())
                                .collect()
                        })
                        .unwrap_or_default();
                    result
                        .as_object_mut()
                        .unwrap()
                        .insert("interrupt_ids".into(), json!(ids));
                }
                Ok(result)
            }
            Command::FinalStop(_) => {
                if !gates.all_positive() {
                    return Err(Error("control_gate_not_retired"));
                }
                self.finished = true;
                Ok(json!({"finished":true}))
            }
        }
    }
}
pub fn serve(
    mut reader: impl Read,
    mut writer: impl Write,
    store: Arc<SqliteStore>,
    gates: Gates,
    evidence: Evidence,
    base: String,
    token: String,
) -> Result<Proof> {
    let mut session = Session::new();
    while let Some(line) = read_line(&mut reader)? {
        let envelope = decode(line.strip_suffix(b"\n").unwrap())?;
        evidence.write(
            &format!("control-{:04}-request.json", envelope.ordinal),
            &line,
        )?;
        let result = session.execute(&envelope, &store, &gates, &evidence, &base, &token);
        let reply = match &result {
            Ok(value) => json!({"version":1,"ordinal":envelope.ordinal,"ok":true,"result":value}),
            Err(error) => {
                json!({"version":1,"ordinal":envelope.ordinal,"ok":false,"error":error.0})
            }
        };
        let encoded = files::encode(&reply, LINE)?;
        evidence.write(
            &format!("control-{:04}-reply.json", envelope.ordinal),
            &encoded,
        )?;
        writer
            .write_all(&encoded)
            .and_then(|_| writer.flush())
            .map_err(|_| Error("control_write_failed"))?;
        result?;
    }
    if !session.finished {
        return Err(Error("control_eof_before_finish"));
    }
    Ok(Proof {
        commands: session.ordinal,
        final_stop: true,
        pipe_eof: true,
        observed_threads: session.threads.into_iter().collect(),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn valid_versioned_ready_is_accepted() {
        assert!(
            decode(br#"{"version":1,"ordinal":1,"case":"setup","command":{"ready":{}}}"#).is_ok()
        );
    }
    #[test]
    fn control_rejects_unknown_fields_duplicates_partial_and_oversized_lines() {
        for bytes in [
            br#"{"version":1,"ordinal":1,"case":"setup","command":{"ready":{"extra":1}}}"#
                .as_slice(),
            br#"{"version":1,"ordinal":1,"ordinal":2,"case":"setup","command":{"ready":{}}}"#,
            br#"{"version":1,"ordinal":1,"case":"setup","command":{"ready":{}} ,"extra":1}"#,
        ] {
            assert!(decode(bytes).is_err());
        }
        assert!(read_line(&mut b"partial".as_slice()).is_err());
        assert!(read_line(&mut vec![b'a'; LINE].as_slice()).is_err());
        let mut edge = vec![b' '; LINE - 1];
        edge.push(b'\n');
        assert_eq!(
            read_line(&mut edge.as_slice()).unwrap().unwrap().len(),
            LINE
        );
    }
    #[test]
    fn phase_and_ordinal_validation_reject_repeated_ready_and_stale_commands() {
        let mut s = Session::new();
        let ready =
            decode(br#"{"version":1,"ordinal":1,"case":"setup","command":{"ready":{}}}"#).unwrap();
        s.validate(&ready).unwrap();
        s.ready = true;
        assert!(s.validate(&ready).is_err());
        let bad=decode(br#"{"version":1,"ordinal":2,"case":"C1","command":{"arm":{"gate":"controller_dispatch"}}}"#).unwrap();
        assert!(s.validate(&bad).is_err());
    }
    #[test]
    fn command_limit_is_cumulative_and_exactly_256() {
        let mut s = Session::new();
        let ready =
            decode(br#"{"version":1,"ordinal":1,"case":"setup","command":{"ready":{}}}"#).unwrap();
        s.validate(&ready).unwrap();
        s.ready = true;
        for ordinal in 2..=COMMANDS {
            let request=decode(&serde_json::to_vec(&json!({"version":1,"ordinal":ordinal,"case":"W4","command":{"inspect_thread":{"thread_id":"unit-missing"}}})).unwrap()).unwrap();
            s.validate(&request).unwrap();
        }
        let excess=decode(&serde_json::to_vec(&json!({"version":1,"ordinal":COMMANDS+1,"case":"retirement","command":{"final_stop":{}}})).unwrap()).unwrap();
        assert!(s.validate(&excess).is_err());
        assert_eq!(s.ordinal, COMMANDS);
    }
    #[test]
    fn real_store_control_records_full_snapshots_bounded_replies_and_positive_eof() {
        let base = std::env::temp_dir().join(format!("b4-ap-control-{}", uuid::Uuid::new_v4()));
        files::private_dir(&base).unwrap();
        let state = base.join("state");
        files::prepare_store(&state).unwrap();
        let native = base.join("native");
        files::private_dir(&native).unwrap();
        let evidence = Evidence::new(native.clone());
        let store = Arc::new(SqliteStore::open(&state).unwrap());
        let requests = [
            json!({"version":1,"ordinal":1,"case":"setup","command":{"ready":{}}}),
            json!({"version":1,"ordinal":2,"case":"C1","command":{"inspect_thread":{"thread_id":"unit-missing"}}}),
            json!({"version":1,"ordinal":3,"case":"W4","command":{"inspect_pending":{"thread_id":"unit-missing"}}}),
            json!({"version":1,"ordinal":4,"case":"retirement","command":{"final_stop":{}}}),
        ];
        let input: Vec<_> = requests
            .iter()
            .flat_map(|v| files::encode(v, LINE).unwrap())
            .collect();
        let mut output = Vec::new();
        let proof = serve(
            input.as_slice(),
            &mut output,
            store,
            Gates::new(),
            evidence.clone(),
            "http://127.0.0.1:1".into(),
            "unit-token".into(),
        )
        .unwrap();
        assert_eq!(proof.commands, 4);
        assert!(proof.final_stop && proof.pipe_eof);
        assert_eq!(proof.observed_threads.len(), 1);
        let replies: Vec<Value> = output
            .split(|b| *b == b'\n')
            .filter(|b| !b.is_empty())
            .map(|b| {
                assert!(b.len() + 1 <= LINE);
                serde_json::from_slice(b).unwrap()
            })
            .collect();
        assert_eq!(replies.len(), 4);
        assert_eq!(replies[1]["result"]["found"], false);
        assert!(replies[2]["result"]["interrupt_ids"]
            .as_array()
            .unwrap()
            .is_empty());
        assert_eq!(replies[3]["result"], json!({"finished":true}));
        assert_eq!(
            std::fs::read(native.join("snapshot-0003-approval.json")).unwrap(),
            b"null\n"
        );
        assert!(evidence.finish().unwrap() > 0);
        std::fs::remove_dir_all(base).unwrap();
    }
}
