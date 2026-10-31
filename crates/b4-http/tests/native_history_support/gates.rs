use super::WAIT;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, Default)]
pub struct GateFacts {
    pub entered: bool,
    pub explicit: bool,
    pub returned: bool,
    pub expired: bool,
    pub cleanup: bool,
}
pub struct Gate {
    deadline: Instant,
    state: Mutex<GateFacts>,
    changed: Condvar,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + Duration::from_secs(45),
            state: Mutex::new(GateFacts::default()),
            changed: Condvar::new(),
        })
    }
    pub fn hold(&self) -> bool {
        let mut f = self.state.lock().unwrap();
        f.entered = true;
        self.changed.notify_all();
        while !f.explicit && !f.cleanup && !f.expired {
            let left = self.deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                f.expired = true;
                break;
            }
            f = self.changed.wait_timeout(f, left).unwrap().0;
        }
        if Instant::now() >= self.deadline {
            f.expired = true;
        }
        f.returned = true;
        self.changed.notify_all();
        f.explicit && !f.cleanup && !f.expired
    }
    pub async fn entered(&self) {
        tokio::time::timeout(WAIT, async {
            while !self.facts().entered {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual entry remained finite");
        assert!(!self.facts().expired && !self.facts().cleanup);
    }
    pub fn release(&self) {
        let mut f = self.state.lock().unwrap();
        f.explicit = true;
        if Instant::now() >= self.deadline {
            f.expired = true;
        }
        self.changed.notify_all();
    }
    pub fn facts(&self) -> GateFacts {
        *self.state.lock().unwrap()
    }
    pub fn positive(&self) {
        let f = self.facts();
        assert!(
            f.entered && f.explicit && f.returned && !f.expired && !f.cleanup,
            "{f:?}"
        );
    }
    fn cleanup(&self) {
        let mut f = self.state.lock().unwrap();
        if !f.explicit {
            f.cleanup = true;
        }
        self.changed.notify_all();
    }
}
pub struct GateCleanup(pub Arc<Gate>);
impl Drop for GateCleanup {
    fn drop(&mut self) {
        self.0.cleanup();
    }
}
