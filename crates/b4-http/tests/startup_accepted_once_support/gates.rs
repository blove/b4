use super::{evidence::now_ms, model::*, GATE_WAIT, WAIT};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GateWitness {
    pub facts: GateFacts,
    pub entered_at_ms: Option<u64>,
    pub returned_at_ms: Option<u64>,
    /// Actual monotonic lifetime remaining when this pure observation is taken.
    pub remaining_ms: u64,
}
struct State {
    witness: GateWitness,
    deadline: Instant,
}
pub struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
impl Gate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                witness: GateWitness {
                    facts: GateFacts::default(),
                    entered_at_ms: None,
                    returned_at_ms: None,
                    remaining_ms: u64::try_from(GATE_WAIT.as_millis()).expect("fixed gate bound"),
                },
                deadline: Instant::now() + GATE_WAIT,
            }),
            changed: Condvar::new(),
        })
    }
    fn expire(s: &mut State) {
        if !s.witness.facts.returned && Instant::now() >= s.deadline {
            s.witness.facts.expired = true;
        }
    }
    pub fn hold(&self) -> bool {
        let Ok(mut s) = self.state.lock() else {
            return false;
        };
        Self::expire(&mut s);
        if s.witness.facts.entered || s.witness.facts.explicit || s.witness.facts.cleanup {
            return false;
        }
        s.witness.facts.entered = true;
        s.witness.entered_at_ms = now_ms().ok();
        if s.witness.entered_at_ms.is_none() {
            s.witness.facts.cleanup = true;
        }
        self.changed.notify_all();
        while !s.witness.facts.explicit && !s.witness.facts.cleanup && !s.witness.facts.expired {
            let remaining = s.deadline.saturating_duration_since(Instant::now());
            let Ok(next) = self.changed.wait_timeout(s, remaining) else {
                return false;
            };
            s = next.0;
            Self::expire(&mut s);
        }
        Self::expire(&mut s);
        s.witness.facts.returned = true;
        s.witness.returned_at_ms = now_ms().ok();
        if s.witness.returned_at_ms.is_none() {
            s.witness.facts.cleanup = true;
        }
        self.changed.notify_all();
        s.witness.facts.positive()
    }
    pub fn witness(&self) -> Result<GateWitness> {
        let mut s = self.state.lock().map_err(|_| Failure::Setup)?;
        Self::expire(&mut s);
        let mut observed = s.witness.clone();
        observed.remaining_ms = u64::try_from(
            s.deadline
                .saturating_duration_since(Instant::now())
                .as_millis(),
        )
        .map_err(|_| Failure::Bound)?;
        Ok(observed)
    }
    pub fn facts(&self) -> Result<GateFacts> {
        Ok(self.witness()?.facts)
    }
    pub fn release(&self) -> Result<()> {
        let mut s = self.state.lock().map_err(|_| Failure::Setup)?;
        Self::expire(&mut s);
        if !s.witness.facts.entered
            || s.witness.facts.returned
            || s.witness.facts.expired
            || s.witness.facts.cleanup
            || s.witness.facts.explicit
        {
            return Err(Failure::Protocol);
        }
        s.witness.facts.explicit = true;
        self.changed.notify_all();
        Ok(())
    }
    pub fn cleanup(&self) {
        if let Ok(mut s) = self.state.lock() {
            s.witness.facts.cleanup = true;
            self.changed.notify_all();
        }
    }
    pub async fn entered(&self) -> Result<()> {
        until(|| self.facts().is_ok_and(|f| f.entered)).await?;
        let f = self.facts()?;
        if f.expired || f.cleanup {
            Err(Failure::Timeout)
        } else {
            Ok(())
        }
    }
    pub async fn returned(&self) -> Result<()> {
        until(|| self.facts().is_ok_and(|f| f.returned)).await?;
        if self.facts()?.positive() {
            Ok(())
        } else {
            Err(Failure::Cleanup)
        }
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.cleanup();
    }
}

/// Error cleanup is separate from every positive explicit release witness.
pub struct Cleanup {
    gates: Vec<Arc<Gate>>,
    armed: bool,
}
impl Cleanup {
    pub fn new(gates: Vec<Arc<Gate>>) -> Self {
        Self { gates, armed: true }
    }
    pub fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.armed {
            for g in &self.gates {
                g.cleanup();
            }
        }
    }
}
pub async fn until(mut predicate: impl FnMut() -> bool) -> Result<()> {
    tokio::time::timeout(WAIT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .map_err(|_| Failure::Timeout)
}

#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Clock {
    pub observed_at_ms: u64,
}
impl Clock {
    pub fn sample() -> Result<Self> {
        Ok(Self {
            observed_at_ms: now_ms()?,
        })
    }
}
#[derive(Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expiry {
    pub expires_at_ms: u64,
    pub observed_at_ms: u64,
}
pub async fn actual_expiry(expires_at_ms: u64) -> Result<Expiry> {
    let deadline = Instant::now() + WAIT;
    loop {
        let observed_at_ms = now_ms()?;
        if observed_at_ms > expires_at_ms {
            return Ok(Expiry {
                expires_at_ms,
                observed_at_ms,
            });
        }
        if Instant::now() >= deadline {
            return Err(Failure::Timeout);
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}
