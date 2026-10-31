use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

const WAIT: Duration = Duration::from_secs(15);
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Facts {
    pub entered: bool,
    pub explicit: bool,
    pub returned: bool,
    pub expired: bool,
    pub cleanup: bool,
}
pub(super) struct Gate {
    deadline: Instant,
    facts: Mutex<Facts>,
    changed: Condvar,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + WAIT,
            facts: Mutex::new(Facts::default()),
            changed: Condvar::new(),
        })
    }
    pub fn hold(&self) -> bool {
        let mut facts = self.facts.lock().unwrap();
        facts.entered = true;
        self.changed.notify_all();
        while !facts.explicit && !facts.cleanup && !facts.expired {
            let remaining = self.deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                facts.expired = true;
                break;
            }
            facts = self.changed.wait_timeout(facts, remaining).unwrap().0;
        }
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.returned = true;
        self.changed.notify_all();
        facts.explicit && !facts.expired && !facts.cleanup
    }
    pub async fn entered(&self) {
        tokio::time::timeout(WAIT, async {
            while !self.facts().entered {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("finite actual gate entry");
        assert!(!self.facts().expired);
    }
    pub fn release(&self) {
        let mut facts = self.facts.lock().unwrap();
        facts.explicit = true;
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        self.changed.notify_all();
    }
    pub fn facts(&self) -> Facts {
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
        let mut facts = self.facts.lock().unwrap();
        if !facts.explicit {
            facts.cleanup = true;
        }
        self.changed.notify_all();
    }
}
pub(super) struct Cleanup(pub Arc<Gate>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleanup();
    }
}
