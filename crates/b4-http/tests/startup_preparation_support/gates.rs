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
    facts: Mutex<GateFacts>,
    changed: Condvar,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + Duration::from_secs(45),
            facts: Mutex::new(GateFacts::default()),
            changed: Condvar::new(),
        })
    }
    pub fn hold(&self) -> bool {
        let mut f = self.facts.lock().unwrap();
        f.entered = true;
        if Instant::now() >= self.deadline {
            f.expired = true;
        }
        self.changed.notify_all();
        while !f.explicit && !f.cleanup && !f.expired {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                f.expired = true;
                break;
            }
            f = self.changed.wait_timeout(f, remaining).unwrap().0;
        }
        if Instant::now() >= self.deadline {
            f.expired = true;
        }
        f.returned = true;
        self.changed.notify_all();
        f.explicit && !f.expired && !f.cleanup
    }
    pub async fn entered(&self) {
        tokio::time::timeout(WAIT, async {
            while !self.facts().entered {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual gate entry remained finite");
        assert!(!self.facts().expired);
    }
    pub fn release(&self) {
        let mut f = self.facts.lock().unwrap();
        f.explicit = true;
        if Instant::now() >= self.deadline {
            f.expired = true;
        }
        self.changed.notify_all();
    }
    pub fn facts(&self) -> GateFacts {
        *self.facts.lock().unwrap()
    }
    pub fn positive(&self) {
        let f = self.facts();
        assert!(
            f.entered && f.explicit && f.returned && !f.expired && !f.cleanup,
            "{f:?}"
        );
    }
    fn cleanup(&self) {
        let mut f = self.facts.lock().unwrap();
        if !f.explicit {
            f.cleanup = true;
        }
        self.changed.notify_all();
    }
}

pub struct Cleanup(pub Arc<Gate>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleanup();
    }
}
