use crate::control::RunControl;
use b4_contract::ThreadId;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub(crate) enum ClaimError {
    Busy,
    Poisoned,
}
#[derive(Default)]
pub(crate) struct ClaimRegistry {
    entries: BTreeMap<ThreadId, Entry>,
}
struct Entry {
    token: uuid::Uuid,
    control: Option<Arc<RunControl>>,
}
#[derive(Clone)]
pub(crate) struct CapturedRun {
    token: uuid::Uuid,
    pub control: Arc<RunControl>,
}
impl ClaimRegistry {
    pub(crate) fn capture(&self, id: &ThreadId) -> Option<CapturedRun> {
        let entry = self.entries.get(id)?;
        Some(CapturedRun {
            token: entry.token,
            control: entry.control.clone()?,
        })
    }
    pub(crate) fn contains(&self, id: &ThreadId, run: &CapturedRun) -> bool {
        self.entries.get(id).is_some_and(|entry| {
            entry.token == run.token
                && entry
                    .control
                    .as_ref()
                    .is_some_and(|control| Arc::ptr_eq(control, &run.control))
        })
    }
}
pub(crate) struct ThreadClaim {
    claims: Arc<Mutex<ClaimRegistry>>,
    thread_id: ThreadId,
    token: uuid::Uuid,
}
impl ThreadClaim {
    pub(crate) fn acquire(
        claims: Arc<Mutex<ClaimRegistry>>,
        thread_id: ThreadId,
    ) -> Result<Self, ClaimError> {
        let token = uuid::Uuid::new_v4();
        {
            let mut held = claims.lock().map_err(|_| ClaimError::Poisoned)?;
            if held.entries.contains_key(&thread_id) {
                return Err(ClaimError::Busy);
            }
            held.entries.insert(
                thread_id.clone(),
                Entry {
                    token,
                    control: None,
                },
            );
        }
        Ok(Self {
            claims,
            thread_id,
            token,
        })
    }
    pub(crate) fn publish(&self, control: Arc<RunControl>) -> Result<(), ClaimError> {
        let mut held = self.claims.lock().map_err(|_| ClaimError::Poisoned)?;
        let entry = held
            .entries
            .get_mut(&self.thread_id)
            .filter(|entry| entry.token == self.token)
            .ok_or(ClaimError::Poisoned)?;
        entry.control = Some(control);
        Ok(())
    }
}
impl Drop for ThreadClaim {
    fn drop(&mut self) {
        let mut held = self
            .claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if held
            .entries
            .get(&self.thread_id)
            .is_some_and(|entry| entry.token == self.token)
        {
            held.entries.remove(&self.thread_id);
        }
    }
}
