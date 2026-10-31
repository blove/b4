use axum::{
    http::header,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
};
use serde_json::Value;
use std::{
    collections::VecDeque,
    convert::Infallible,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};

#[derive(Clone)]
pub(crate) struct Frame {
    pub name: &'static str,
    pub data: Arc<str>,
    pub bytes: usize,
}
impl Frame {
    pub(crate) fn new(name: &'static str, data: &Value) -> Self {
        let data: Arc<str> = data.to_string().into();
        let bytes = 16 + name.len() + data.len();
        Self { name, data, bytes }
    }
    fn event(&self) -> Event {
        Event::default().event(self.name).data(&self.data)
    }
}
struct Queue {
    frames: VecDeque<Frame>,
    bytes: usize,
    closed: bool,
    overflow: bool,
    signalled: bool,
    waker: Option<Waker>,
}
#[derive(Clone)]
pub(crate) struct ViewerSender {
    state: Arc<Mutex<Queue>>,
    max_frames: usize,
    max_bytes: usize,
}
pub(crate) struct ViewerStream {
    state: Arc<Mutex<Queue>>,
    prefix: VecDeque<Event>,
    guard: Option<crate::control::SubscriptionGuard>,
}
pub(crate) fn channel(max_frames: usize, max_bytes: usize) -> (ViewerSender, ViewerStream) {
    let state = Arc::new(Mutex::new(Queue {
        frames: VecDeque::new(),
        bytes: 0,
        closed: false,
        overflow: false,
        signalled: false,
        waker: None,
    }));
    (
        ViewerSender {
            state: state.clone(),
            max_frames,
            max_bytes,
        },
        ViewerStream {
            state,
            prefix: VecDeque::new(),
            guard: None,
        },
    )
}
impl ViewerSender {
    pub(crate) fn send(&self, frame: Frame) -> bool {
        let (sent, waker) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return false;
            }
            let sent = state.frames.len() < self.max_frames
                && state
                    .bytes
                    .checked_add(frame.bytes)
                    .is_some_and(|bytes| bytes <= self.max_bytes);
            if sent {
                state.bytes += frame.bytes;
                state.frames.push_back(frame);
            } else {
                state.frames.clear();
                state.bytes = 0;
                state.overflow = true;
                state.closed = true;
            }
            (sent, state.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        sent
    }
    pub(crate) fn close(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    pub(crate) fn discard(&self) {
        let waker = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.frames.clear();
            state.bytes = 0;
            state.closed = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
impl ViewerStream {
    pub(crate) fn guard(&mut self, guard: crate::control::SubscriptionGuard) {
        self.guard = Some(guard);
    }
    pub(crate) fn prefix(&mut self, frames: Vec<Event>) {
        self.prefix.extend(frames);
    }
    pub(crate) fn finite(frames: Vec<Event>) -> Self {
        let (sender, mut stream) = channel(1, 1);
        sender.close();
        stream.prefix(frames);
        stream
    }
}
impl futures_core::Stream for ViewerStream {
    type Item = Result<Event, Infallible>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(frame) = self.prefix.pop_front() {
            return Poll::Ready(Some(Ok(frame)));
        }
        let result = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.overflow && !state.signalled {
                state.signalled = true;
                Poll::Ready(Some(Ok(Event::default()
                    .event("detached")
                    .data("{\"reason\":\"overflow\"}"))))
            } else if let Some(frame) = state.frames.pop_front() {
                state.bytes -= frame.bytes;
                Poll::Ready(Some(Ok(frame.event())))
            } else if state.closed {
                Poll::Ready(None)
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        };
        if matches!(result, Poll::Ready(None)) {
            self.guard = None;
        }
        result
    }
}
impl Drop for ViewerStream {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.frames.clear();
        state.bytes = 0;
        state.closed = true;
        let waker = state.waker.take();
        drop(state);
        drop(waker);
        self.guard = None;
    }
}

pub(crate) fn response(stream: ViewerStream, heartbeat_ms: u64) -> Response {
    let mut response = Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_millis(heartbeat_ms))
                .text("ping"),
        )
        .into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-store, no-transform".parse().unwrap(),
    );
    response
        .headers_mut()
        .insert(header::CONNECTION, "keep-alive".parse().unwrap());
    response
        .headers_mut()
        .insert("x-accel-buffering", "no".parse().unwrap());
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use serde_json::json;
    #[tokio::test]
    async fn utf8_byte_overflow_discards_queue_and_delivers_one_independent_detachment() {
        let frame = Frame::new("chunk", &json!("😀"));
        let (sender, stream) = channel(4, frame.bytes * 2 - 1);
        assert!(sender.send(frame.clone()));
        assert!(!sender.send(frame));
        sender.close();
        let body = to_bytes(response(stream, 1000).into_body(), 1000)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(body.to_vec()).unwrap(),
            "event: detached\ndata: {\"reason\":\"overflow\"}\n\n"
        );
    }
    #[tokio::test]
    async fn dropping_a_quiet_receiver_makes_publication_inert() {
        let (sender, stream) = channel(2, 1024);
        drop(stream);
        assert!(!sender.send(Frame::new("chunk", &json!("unobserved"))));
    }
}
