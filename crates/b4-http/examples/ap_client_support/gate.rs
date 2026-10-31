use super::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

pub const LIFETIME: Duration = Duration::from_secs(30);
pub const WAIT: Duration = Duration::from_secs(10);
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GateId {
    ControllerDispatch,
    WorkerHeld,
}
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct GateStatus {
    pub generation: u64,
    pub entered: bool,
    pub returned: bool,
    pub explicit_release: bool,
    pub cleanup_release: bool,
    pub expired: bool,
}
struct State {
    status: GateStatus,
    armed: bool,
    deadline: Option<Instant>,
    failed: bool,
}
pub struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
impl Gate {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                status: GateStatus::default(),
                armed: false,
                deadline: None,
                failed: false,
            }),
            changed: Condvar::new(),
        }
    }
    pub fn arm(&self) -> Result<u64> {
        self.arm_for(LIFETIME)
    }
    fn arm_for(&self, lifetime: Duration) -> Result<u64> {
        let mut s = self.state.lock().map_err(|_| Error("gate_poisoned"))?;
        if s.failed || s.armed && !s.status.returned {
            return Err(Error("gate_invalid_phase"));
        }
        let generation = s
            .status
            .generation
            .checked_add(1)
            .ok_or(Error("gate_generation_exhausted"))?;
        s.status = GateStatus {
            generation,
            ..GateStatus::default()
        };
        s.armed = true;
        s.deadline = Some(Instant::now() + lifetime);
        Ok(generation)
    }
    pub fn enter(&self) -> Result<Option<ReturnWitness<'_>>> {
        let mut s = self.state.lock().map_err(|_| Error("gate_poisoned"))?;
        if !s.armed || s.status.returned {
            return Ok(None);
        }
        if s.status.entered {
            return Err(Error("gate_duplicate_entry"));
        }
        s.status.entered = true;
        self.changed.notify_all();
        while !s.status.explicit_release && !s.status.cleanup_release {
            let remaining = s
                .deadline
                .unwrap()
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                s.status.expired = true;
                s.failed = true;
                break;
            }
            s = self
                .changed
                .wait_timeout(s, remaining)
                .map_err(|_| Error("gate_poisoned"))?
                .0;
        }
        Ok(Some(ReturnWitness { gate: self }))
    }
    pub fn status(&self, generation: u64) -> Result<GateStatus> {
        let s = self.state.lock().map_err(|_| Error("gate_poisoned"))?;
        if !s.armed || s.status.generation != generation {
            return Err(Error("gate_stale_generation"));
        }
        Ok(s.status)
    }
    fn wait_flag(&self, generation: u64, returned: bool) -> Result<GateStatus> {
        let deadline = Instant::now() + WAIT;
        let mut s = self.state.lock().map_err(|_| Error("gate_poisoned"))?;
        loop {
            if !s.armed || s.status.generation != generation {
                return Err(Error("gate_stale_generation"));
            }
            if s.failed || s.status.expired || s.status.cleanup_release {
                return Err(Error("gate_failed"));
            }
            if if returned {
                s.status.returned
            } else {
                s.status.entered
            } {
                return Ok(s.status);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                s.failed = true;
                return Err(Error("gate_wait_expired"));
            }
            s = self
                .changed
                .wait_timeout(s, remaining)
                .map_err(|_| Error("gate_poisoned"))?
                .0;
        }
    }
    pub fn await_entered(&self, generation: u64) -> Result<GateStatus> {
        self.wait_flag(generation, false)
    }
    pub fn release(&self, generation: u64) -> Result<GateStatus> {
        {
            let mut s = self.state.lock().map_err(|_| Error("gate_poisoned"))?;
            if !s.armed || s.status.generation != generation {
                return Err(Error("gate_stale_generation"));
            }
            if s.deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                s.status.expired = true;
                s.failed = true;
                self.changed.notify_all();
            }
            if !s.status.entered
                || s.status.explicit_release
                || s.status.returned
                || s.status.expired
                || s.failed
                || s.status.cleanup_release
            {
                return Err(Error("gate_invalid_phase"));
            }
            s.status.explicit_release = true;
            self.changed.notify_all();
        }
        self.wait_flag(generation, true)
    }
    pub fn cleanup(&self) {
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.armed && !s.status.returned {
            s.status.cleanup_release = true;
            s.failed = true;
            self.changed.notify_all();
        }
    }
    pub fn latest(&self) -> Option<GateStatus> {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.armed.then_some(s.status)
    }
    pub fn all_positive(&self) -> bool {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        !s.failed
            && (!s.armed
                || s.status.entered
                    && s.status.returned
                    && s.status.explicit_release
                    && !s.status.cleanup_release
                    && !s.status.expired)
    }
}
pub struct ReturnWitness<'a> {
    gate: &'a Gate,
}
impl Drop for ReturnWitness<'_> {
    fn drop(&mut self) {
        let mut s = self.gate.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            s.status.expired = true;
            s.failed = true;
        }
        s.status.returned = true;
        self.gate.changed.notify_all();
    }
}
#[derive(Clone)]
pub struct Gates {
    pub controller: Arc<Gate>,
    pub worker: Arc<Gate>,
}
impl Gates {
    pub fn new() -> Self {
        Self {
            controller: Arc::new(Gate::new()),
            worker: Arc::new(Gate::new()),
        }
    }
    pub fn get(&self, id: GateId) -> &Gate {
        match id {
            GateId::ControllerDispatch => &self.controller,
            GateId::WorkerHeld => &self.worker,
        }
    }
    pub fn all_positive(&self) -> bool {
        self.controller.all_positive() && self.worker.all_positive()
    }
    pub fn cleanup(&self) {
        self.controller.cleanup();
        self.worker.cleanup();
    }
}
pub struct Cleanup(pub Gates);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn expiry_followed_by_release_is_not_positive_release() {
        let gate = Gate::new();
        let generation = gate.arm_for(Duration::ZERO).unwrap();
        drop(gate.enter().unwrap());
        assert!(gate.status(generation).unwrap().expired);
        assert!(gate.release(generation).is_err());
        assert!(!gate.all_positive());
    }
    #[test]
    fn entered_explicit_release_witnesses_return_and_rejects_stale_generation() {
        let gate = Arc::new(Gate::new());
        let generation = gate.arm().unwrap();
        let other = gate.clone();
        let task = std::thread::spawn(move || {
            let _returned = other.enter().unwrap();
        });
        assert!(gate.await_entered(generation).unwrap().entered);
        let result = gate.release(generation).unwrap();
        assert!(result.returned && result.explicit_release && !result.expired);
        task.join().unwrap();
        assert!(gate.all_positive());
        let next = gate.arm().unwrap();
        assert!(next > generation);
        assert!(gate.status(generation).is_err());
        gate.cleanup();
    }
    #[test]
    fn cleanup_release_never_counts_as_explicit_positive_retirement() {
        let gate = Arc::new(Gate::new());
        let generation = gate.arm().unwrap();
        let other = gate.clone();
        let task = std::thread::spawn(move || {
            let _returned = other.enter().unwrap();
        });
        gate.await_entered(generation).unwrap();
        gate.cleanup();
        task.join().unwrap();
        let flags = gate.status(generation).unwrap();
        assert!(flags.cleanup_release && flags.returned && !flags.explicit_release);
        assert!(!gate.all_positive());
    }
    #[test]
    fn explicit_release_before_deadline_cannot_qualify_actual_late_return() {
        let gate = Arc::new(Gate::new());
        let generation = gate.arm().unwrap();
        let callback_gate = gate.clone();
        let (held, actual_hold) = std::sync::mpsc::sync_channel(1);
        let (return_now, actual_return) = std::sync::mpsc::sync_channel(1);
        let callback = std::thread::spawn(move || {
            let witness = callback_gate.enter().unwrap();
            held.send(()).unwrap();
            actual_return.recv_timeout(WAIT).unwrap();
            drop(witness);
        });
        gate.await_entered(generation).unwrap();
        let release_gate = gate.clone();
        let release = std::thread::spawn(move || release_gate.release(generation));
        actual_hold.recv_timeout(WAIT).unwrap();
        let before = gate.status(generation).unwrap();
        assert!(before.entered && before.explicit_release && !before.returned && !before.expired);
        // Advance only the private test deadline after real release and entry.
        // The real ReturnWitness remains held, so its eventual drop is late.
        gate.state.lock().unwrap().deadline = Some(Instant::now() - Duration::from_secs(1));
        return_now.send(()).unwrap();
        callback.join().unwrap();
        let result = release.join().unwrap();
        assert!(
            result.is_err(),
            "an actual late return must fail the waiting release"
        );
        let after = gate.status(generation).unwrap();
        assert!(after.returned && after.explicit_release && after.expired);
        assert!(!gate.all_positive());
        assert!(gate.arm().is_err());
    }
}
