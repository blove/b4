use super::{Gate, Owner};
use b4_http::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
pub struct CancelPolicy {
    pub calls: AtomicUsize,
    pub deny_at: Option<usize>,
    pub hold_at: Option<(usize, Arc<Gate>)>,
}
impl CancelPolicy {
    pub fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            deny_at: None,
            hold_at: None,
        }
    }
}
impl ResourcePolicy for CancelPolicy {
    fn authorize(&self, p: &Principal, c: &ResourceContext) -> bool {
        if c.operation == Operation::CancelRun {
            let ordinal = self.calls.fetch_add(1, Ordering::AcqRel);
            if let Some((held, gate)) = &self.hold_at {
                if ordinal == *held && !gate.hold() {
                    return false;
                }
            }
            if self.deny_at == Some(ordinal) {
                return false;
            }
        }
        Owner.authorize(p, c)
    }
}
