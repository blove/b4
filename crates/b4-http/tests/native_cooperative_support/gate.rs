use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Default, Debug)]
pub struct GateFacts {
    pub entered: bool,
    pub explicit: bool,
    pub cleanup: bool,
    pub expired: bool,
}

pub struct Gate {
    deadline: Instant,
    facts: Mutex<GateFacts>,
    changed: Condvar,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + Duration::from_secs(30),
            facts: Mutex::new(GateFacts::default()),
            changed: Condvar::new(),
        })
    }
    #[cfg(test)]
    fn with_lifetime(lifetime: Duration) -> Arc<Self> {
        Arc::new(Self {
            deadline: Instant::now() + lifetime,
            facts: Mutex::new(GateFacts::default()),
            changed: Condvar::new(),
        })
    }
    pub fn hold(&self) -> bool {
        let mut facts = self.facts.lock().unwrap();
        facts.entered = true;
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
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
        facts.explicit && !facts.cleanup && !facts.expired
    }
    pub fn release(&self) {
        let mut facts = self.facts.lock().unwrap();
        if Instant::now() >= self.deadline {
            facts.expired = true;
        }
        facts.explicit = true;
        self.changed.notify_all();
    }
    pub fn facts(&self) -> GateFacts {
        *self.facts.lock().unwrap()
    }
    fn cleanup(&self) {
        let mut facts = self.facts.lock().unwrap();
        if !facts.explicit {
            facts.cleanup = true;
        }
        self.changed.notify_all();
    }
}

#[cfg(test)]
#[path = "gate_tests.rs"]
mod tests;
pub struct Cleanup(pub Arc<Gate>);
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.0.cleanup();
    }
}
