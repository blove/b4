use super::DriverConfigError;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

/// Finite process-local callback capacity shared by independent owned drivers.
#[derive(Debug)]
pub struct TaskCapacity {
    available: Mutex<usize>,
    changed: Condvar,
    limit: usize,
}
impl TaskCapacity {
    pub fn new(limit: usize) -> Result<Self, DriverConfigError> {
        if !(1..=256).contains(&limit) {
            return Err(DriverConfigError("invalid_task_capacity"));
        }
        Ok(Self {
            available: Mutex::new(limit),
            changed: Condvar::new(),
            limit,
        })
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub(crate) fn try_acquire(&self) -> Option<TaskPermit<'_>> {
        let mut available = self.available.lock().unwrap_or_else(|e| e.into_inner());
        if *available == 0 {
            return None;
        }
        *available -= 1;
        Some(TaskPermit { capacity: self })
    }
    // Only used when this coordinator owns no callbacks or queued outcomes.
    pub(crate) fn wait_for_change(&self) {
        let available = self.available.lock().unwrap_or_else(|e| e.into_inner());
        if *available == 0 {
            drop(
                self.changed
                    .wait_timeout(available, Duration::from_millis(10)),
            );
        }
    }
}
pub(crate) struct TaskPermit<'a> {
    capacity: &'a TaskCapacity,
}
impl Drop for TaskPermit<'_> {
    fn drop(&mut self) {
        let mut available = self
            .capacity
            .available
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *available += 1;
        self.capacity.changed.notify_one();
    }
}
