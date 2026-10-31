use super::{
    buffer::{Event, MethodLabel, Record, RequestId},
    Inner, REQUEST_ID_HEADER,
};
use axum::{
    http::{HeaderValue, Method},
    response::Response,
};
use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

pub(crate) struct RequestObservation {
    inner: Arc<Inner>,
    pending: Option<RequestId>,
    method: MethodLabel,
    started: Instant,
}

impl RequestObservation {
    pub(super) fn new(inner: Arc<Inner>, method: &Method) -> Self {
        let started = Instant::now();
        let pending = inner.next_id();
        let method = match *method {
            Method::GET => MethodLabel::Get,
            Method::POST => MethodLabel::Post,
            Method::DELETE => MethodLabel::Delete,
            _ => MethodLabel::Other,
        };
        Self {
            inner,
            pending,
            method,
            started,
        }
    }

    pub(crate) fn finish(mut self, response: &mut Response) {
        #[cfg(test)]
        {
            let gate = self.inner.finish_gate.lock().unwrap().clone();
            if let Some(gate) = gate {
                gate.pause();
            }
        }
        let Some(request_id) = self.pending.take() else {
            return;
        };
        let Ok(header) = HeaderValue::from_bytes(&request_id.0) else {
            self.inner.record_loss.store(true, Ordering::Relaxed);
            return;
        };
        if response
            .headers_mut()
            .try_insert(REQUEST_ID_HEADER, header)
            .is_err()
        {
            self.inner.record_loss.store(true, Ordering::Relaxed);
            return;
        }
        let record = Record {
            request_id,
            event: Event::ResponseReady,
            method: self.method,
            status_code: Some(response.status().as_u16()),
            elapsed_ms: elapsed_ms(self.started.elapsed()),
        };
        self.inner.observe(record);
    }
}

impl Drop for RequestObservation {
    fn drop(&mut self) {
        if let Some(request_id) = self.pending.take() {
            let record = Record {
                request_id,
                event: Event::RequestFutureDropped,
                method: self.method,
                status_code: None,
                elapsed_ms: elapsed_ms(self.started.elapsed()),
            };
            self.inner.observe(record);
        }
    }
}

pub(super) fn elapsed_ms(elapsed: Duration) -> u64 {
    elapsed.as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
pub(super) struct FinishGate {
    state: std::sync::Mutex<FinishGateState>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
#[derive(Default)]
struct FinishGateState {
    entered: bool,
    released: bool,
    expired: bool,
}

#[cfg(test)]
impl FinishGate {
    const WAIT: Duration = Duration::from_secs(10);

    pub(super) fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(FinishGateState::default()),
            changed: std::sync::Condvar::new(),
        }
    }

    fn pause(&self) {
        self.pause_for(Self::WAIT);
    }

    pub(super) fn pause_for(&self, wait: Duration) {
        let mut state = self.state.lock().unwrap();
        state.entered = true;
        self.changed.notify_all();
        let deadline = Instant::now() + wait;
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.expired = true;
                break;
            }
            let (next, _) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
        }
    }

    pub(super) fn wait_entered(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        let deadline = Instant::now() + Self::WAIT;
        while !state.entered {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, _) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
        }
        !state.expired
    }

    pub(super) fn was_explicitly_released(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.released && !state.expired
    }

    pub(super) fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.changed.notify_all();
    }
}
