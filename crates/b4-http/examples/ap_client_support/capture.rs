use super::{Error, Result};
use serde::Serialize;
use std::{
    collections::VecDeque,
    io::{self, IoSlice},
    net::SocketAddr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{sync_channel, Receiver, SyncSender, TryRecvError},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
pub const SLOTS: usize = 16;
pub const BYTES: usize = 262144;
#[derive(Default)]
struct State {
    accepted: AtomicUsize,
    retired: AtomicUsize,
    loss: AtomicBool,
}
pub struct Record {
    pub ordinal: usize,
    read: Box<[u8]>,
    written: Box<[u8]>,
    read_len: usize,
    write_len: usize,
    pub read_eof: bool,
    pub write_shutdown: bool,
}
impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Record")
            .field("ordinal", &self.ordinal)
            .field("read_len", &self.read_len)
            .field("write_len", &self.write_len)
            .finish()
    }
}
impl Record {
    fn new(size: usize) -> Self {
        Self {
            ordinal: 0,
            read: vec![0; size].into_boxed_slice(),
            written: vec![0; size].into_boxed_slice(),
            read_len: 0,
            write_len: 0,
            read_eof: false,
            write_shutdown: false,
        }
    }
    pub fn read_bytes(&self) -> &[u8] {
        &self.read[..self.read_len]
    }
    pub fn write_bytes(&self) -> &[u8] {
        &self.written[..self.write_len]
    }
    fn copy(&mut self, read: bool, bytes: &[u8]) -> bool {
        let (buffer, used) = if read {
            (&mut self.read, &mut self.read_len)
        } else {
            (&mut self.written, &mut self.write_len)
        };
        let take = bytes.len().min(buffer.len() - *used);
        buffer[*used..*used + take].copy_from_slice(&bytes[..take]);
        *used += take;
        take == bytes.len()
    }
}
pub struct CaptureIo<T = TcpStream> {
    inner: Option<T>,
    record: Option<Record>,
    state: Arc<State>,
    sender: SyncSender<Record>,
}
impl<T> CaptureIo<T> {
    fn record(&mut self, read: bool, bytes: &[u8]) {
        if self.record.as_mut().is_some_and(|r| !r.copy(read, bytes)) {
            self.state.loss.store(true, Ordering::Release);
        }
    }
}
impl<T> Drop for CaptureIo<T> {
    fn drop(&mut self) {
        drop(self.inner.take());
        if let Some(record) = self.record.take() {
            if self.sender.try_send(record).is_ok() {
                if self
                    .state
                    .retired
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
                    .is_err()
                {
                    self.state.loss.store(true, Ordering::Release);
                }
            } else {
                self.state.loss.store(true, Ordering::Release);
            }
        }
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for CaptureIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let remaining = buf.remaining();
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) {
            let bytes = &buf.filled()[before..];
            if bytes.is_empty() && remaining != 0 {
                if let Some(r) = self.record.as_mut() {
                    r.read_eof = true;
                }
            }
            self.record(true, bytes);
        }
        result
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for CaptureIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_write(cx, bytes);
        if let Poll::Ready(Ok(n)) = result {
            self.record(false, &bytes[..n]);
        }
        result
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_write_vectored(cx, bytes);
        if let Poll::Ready(Ok(mut n)) = result {
            for part in bytes {
                let take = n.min(part.len());
                self.record(false, &part[..take]);
                n -= take;
                if n == 0 {
                    break;
                }
            }
        }
        result
    }
    fn is_write_vectored(&self) -> bool {
        self.inner.as_ref().unwrap().is_write_vectored()
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let result = Pin::new(self.inner.as_mut().unwrap()).poll_shutdown(cx);
        if matches!(result, Poll::Ready(Ok(()))) {
            if let Some(r) = self.record.as_mut() {
                r.write_shutdown = true;
            }
        }
        result
    }
}
struct Recorder {
    slots: VecDeque<Record>,
    sender: SyncSender<Record>,
    state: Arc<State>,
}
impl Recorder {
    fn new(slots: usize, size: usize) -> (Self, Collector) {
        let (tx, rx) = sync_channel(slots);
        let state = Arc::new(State::default());
        (
            Self {
                slots: (0..slots).map(|_| Record::new(size)).collect(),
                sender: tx,
                state: state.clone(),
            },
            Collector {
                receiver: rx,
                state,
                limit: slots,
            },
        )
    }
    fn wrap<T>(&mut self, inner: T) -> CaptureIo<T> {
        let ordinal =
            match self
                .state
                .accepted
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            {
                Ok(n) => n + 1,
                Err(_) => {
                    self.state.loss.store(true, Ordering::Release);
                    0
                }
            };
        let mut record = self.slots.pop_front();
        if let Some(r) = &mut record {
            r.ordinal = ordinal;
        } else {
            self.state.loss.store(true, Ordering::Release);
        }
        CaptureIo {
            inner: Some(inner),
            record,
            state: self.state.clone(),
            sender: self.sender.clone(),
        }
    }
}
pub struct CaptureListener {
    inner: TcpListener,
    recorder: Recorder,
}
impl CaptureListener {
    pub fn new(inner: TcpListener) -> (Self, Collector) {
        let (recorder, collector) = Recorder::new(SLOTS, BYTES);
        (Self { inner, recorder }, collector)
    }
}
impl axum::serve::Listener for CaptureListener {
    type Io = CaptureIo;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        let (io, addr) = <TcpListener as axum::serve::Listener>::accept(&mut self.inner).await;
        (self.recorder.wrap(io), addr)
    }
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}
pub struct Collector {
    receiver: Receiver<Record>,
    state: Arc<State>,
    limit: usize,
}
#[derive(Debug, Serialize)]
pub struct CaptureSummary {
    pub accepted: usize,
    pub retired: usize,
    pub sticky_loss: bool,
}
impl Collector {
    pub async fn finish(self) -> Result<(CaptureSummary, Vec<Record>)> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut records = Vec::with_capacity(self.limit);
        loop {
            let accepted = self.state.accepted.load(Ordering::Acquire);
            let retired = self.state.retired.load(Ordering::Acquire);
            if self.state.loss.load(Ordering::Acquire) || accepted > self.limit {
                return Err(Error("capture_incomplete"));
            }
            records.extend(self.receiver.try_iter());
            if accepted == retired
                && records.len() == accepted
                && matches!(self.receiver.try_recv(), Err(TryRecvError::Disconnected))
            {
                return Ok((
                    CaptureSummary {
                        accepted,
                        retired,
                        sticky_loss: false,
                    },
                    records,
                ));
            }
            if Instant::now() >= deadline {
                return Err(Error("capture_retirement_expired"));
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn records_actual_successful_read_and_write_prefixes() {
        let (mut recorder, collector) = Recorder::new(1, 32);
        let (socket, mut peer) = tokio::io::duplex(32);
        peer.write_all(b"actual-read").await.unwrap();
        let mut wrapped = recorder.wrap(socket);
        let mut bytes = [0; 11];
        wrapped.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"actual-read");
        wrapped.write_all(b"actual-write").await.unwrap();
        assert_eq!(
            wrapped.record.as_ref().unwrap().read_bytes(),
            b"actual-read"
        );
        assert_eq!(
            wrapped.record.as_ref().unwrap().write_bytes(),
            b"actual-write"
        );
        drop(wrapped);
        drop(recorder);
        let (s, r) = collector.finish().await.unwrap();
        assert_eq!(s.retired, 1);
        assert_eq!(r.len(), 1);
    }
    #[tokio::test]
    async fn overflow_preserves_actual_transport_and_sets_sticky_loss() {
        let (mut recorder, collector) = Recorder::new(1, 3);
        let (socket, mut peer) = tokio::io::duplex(32);
        let mut io = recorder.wrap(socket);
        io.write_all(b"four").await.unwrap();
        let mut bytes = [0; 4];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"four");
        assert_eq!(io.record.as_ref().unwrap().write_bytes(), b"fou");
        drop(io);
        drop(recorder);
        assert_eq!(
            collector.finish().await.unwrap_err().0,
            "capture_incomplete"
        );
    }
    #[tokio::test]
    async fn exact_capacity_is_complete_and_slots_never_recycle() {
        let (mut recorder, collector) = Recorder::new(1, 3);
        let (socket, _peer) = tokio::io::duplex(32);
        let mut io = recorder.wrap(socket);
        io.write_all(b"abc").await.unwrap();
        drop(io);
        let (socket, _) = tokio::io::duplex(32);
        let next = recorder.wrap(socket);
        drop(next);
        drop(recorder);
        assert_eq!(collector.state.accepted.load(Ordering::Acquire), 2);
        assert!(collector.finish().await.is_err());
    }
}
#[cfg(test)]
mod delegation_tests {
    use super::*;
    use std::task::{Wake, Waker};
    struct Wakeup;
    impl Wake for Wakeup {
        fn wake(self: Arc<Self>) {}
    }
    #[derive(Clone, Copy)]
    enum Step {
        Pending,
        Error,
        Data,
    }
    struct Io {
        read: Step,
        write: Step,
        n: usize,
        flushes: usize,
        shutdowns: usize,
    }
    impl AsyncRead for Io {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            match self.read {
                Step::Pending => Poll::Pending,
                Step::Error => Poll::Ready(Err(io::Error::from(io::ErrorKind::ConnectionReset))),
                Step::Data => {
                    buf.put_slice(b"new");
                    Poll::Ready(Ok(()))
                }
            }
        }
    }
    impl AsyncWrite for Io {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.write {
                Step::Pending => Poll::Pending,
                Step::Error => Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe))),
                Step::Data => Poll::Ready(Ok(self.n)),
            }
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            _: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.poll_write(cx, &[])
        }
        fn is_write_vectored(&self) -> bool {
            true
        }
        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.flushes += 1;
            Poll::Ready(Err(io::Error::from(io::ErrorKind::BrokenPipe)))
        }
        fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.shutdowns += 1;
            Poll::Ready(Ok(()))
        }
    }
    #[test]
    fn prefilled_read_suffix_and_vectored_partial_prefix_are_exact() {
        let (mut recorder, _collector) = Recorder::new(1, 16);
        let mut wrapped = recorder.wrap(Io {
            read: Step::Data,
            write: Step::Data,
            n: 3,
            flushes: 0,
            shutdowns: 0,
        });
        let pointers = (
            wrapped.record.as_ref().unwrap().read.as_ptr(),
            wrapped.record.as_ref().unwrap().written.as_ptr(),
        );
        let waker = Waker::from(Arc::new(Wakeup));
        let mut cx = Context::from_waker(&waker);
        let mut bytes = [0; 16];
        let mut buf = ReadBuf::new(&mut bytes);
        buf.put_slice(b"prior");
        assert!(matches!(
            Pin::new(&mut wrapped).poll_read(&mut cx, &mut buf),
            Poll::Ready(Ok(()))
        ));
        let parts = [
            IoSlice::new(b""),
            IoSlice::new(b"ab"),
            IoSlice::new(b"cdef"),
        ];
        assert!(matches!(
            Pin::new(&mut wrapped).poll_write_vectored(&mut cx, &parts),
            Poll::Ready(Ok(3))
        ));
        assert_eq!(wrapped.record.as_ref().unwrap().read_bytes(), b"new");
        assert_eq!(wrapped.record.as_ref().unwrap().write_bytes(), b"abc");
        assert_eq!(
            pointers,
            (
                wrapped.record.as_ref().unwrap().read.as_ptr(),
                wrapped.record.as_ref().unwrap().written.as_ptr()
            )
        );
        assert!(wrapped.is_write_vectored());
        assert!(matches!(
            Pin::new(&mut wrapped).poll_flush(&mut cx),
            Poll::Ready(Err(_))
        ));
        assert!(matches!(
            Pin::new(&mut wrapped).poll_shutdown(&mut cx),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(wrapped.inner.as_ref().unwrap().flushes, 1);
        assert_eq!(wrapped.inner.as_ref().unwrap().shutdowns, 1);
        assert!(wrapped.record.as_ref().unwrap().write_shutdown);
    }
    #[test]
    fn pending_errors_and_zero_writes_preserve_original_results_without_capture() {
        for step in [Step::Pending, Step::Error] {
            let (mut recorder, _collector) = Recorder::new(1, 16);
            let mut wrapped = recorder.wrap(Io {
                read: step,
                write: step,
                n: 0,
                flushes: 0,
                shutdowns: 0,
            });
            let waker = Waker::from(Arc::new(Wakeup));
            let mut cx = Context::from_waker(&waker);
            let mut bytes = [0; 16];
            let mut buf = ReadBuf::new(&mut bytes);
            buf.put_slice(b"prior");
            let read = Pin::new(&mut wrapped).poll_read(&mut cx, &mut buf);
            let write = Pin::new(&mut wrapped).poll_write(&mut cx, b"not-written");
            match step {
                Step::Pending => assert!(read.is_pending() && write.is_pending()),
                Step::Error => {
                    assert!(
                        matches!(read,Poll::Ready(Err(e))if e.kind()==io::ErrorKind::ConnectionReset)
                    );
                    assert!(
                        matches!(write,Poll::Ready(Err(e))if e.kind()==io::ErrorKind::BrokenPipe)
                    );
                }
                Step::Data => unreachable!(),
            };
            assert!(wrapped.record.as_ref().unwrap().read_bytes().is_empty());
            assert!(wrapped.record.as_ref().unwrap().write_bytes().is_empty());
            wrapped.inner.as_mut().unwrap().write = Step::Data;
            assert!(matches!(
                Pin::new(&mut wrapped).poll_write(&mut cx, b"not-written"),
                Poll::Ready(Ok(0))
            ));
            assert!(wrapped.record.as_ref().unwrap().write_bytes().is_empty());
        }
    }
    struct DropWitness {
        receiver: Arc<std::sync::Mutex<Receiver<Record>>>,
        closed: Arc<AtomicBool>,
    }
    impl Drop for DropWitness {
        fn drop(&mut self) {
            assert!(matches!(
                self.receiver.lock().unwrap().try_recv(),
                Err(TryRecvError::Empty)
            ));
            self.closed.store(true, Ordering::Release);
        }
    }
    #[test]
    fn inner_drop_precedes_capture_transfer_and_successful_retirement_count() {
        let (tx, rx) = sync_channel(1);
        let rx = Arc::new(std::sync::Mutex::new(rx));
        let closed = Arc::new(AtomicBool::new(false));
        let state = Arc::new(State::default());
        state.accepted.store(1, Ordering::Release);
        let wrapped = CaptureIo {
            inner: Some(DropWitness {
                receiver: rx.clone(),
                closed: closed.clone(),
            }),
            record: Some(Record::new(3)),
            state: state.clone(),
            sender: tx,
        };
        drop(wrapped);
        assert!(closed.load(Ordering::Acquire));
        assert!(rx.lock().unwrap().try_recv().is_ok());
        assert_eq!(state.retired.load(Ordering::Acquire), 1);
        assert!(!state.loss.load(Ordering::Acquire));
    }
    #[test]
    fn disconnected_completion_is_sticky_failure_and_never_retired() {
        let (mut recorder, collector) = Recorder::new(1, 3);
        let io = recorder.wrap(());
        let state = recorder.state.clone();
        drop(collector);
        drop(io);
        assert!(state.loss.load(Ordering::Acquire));
        assert_eq!(state.retired.load(Ordering::Acquire), 0);
    }
    #[test]
    fn full_completion_channel_is_sticky_failure_after_inner_drop() {
        let (tx, rx) = sync_channel(1);
        tx.try_send(Record::new(3)).unwrap();
        let closed = Arc::new(AtomicBool::new(false));
        struct Closed(Arc<AtomicBool>);
        impl Drop for Closed {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let state = Arc::new(State::default());
        state.accepted.store(1, Ordering::Release);
        drop(CaptureIo {
            inner: Some(Closed(closed.clone())),
            record: Some(Record::new(3)),
            state: state.clone(),
            sender: tx,
        });
        assert!(closed.load(Ordering::Acquire));
        assert!(state.loss.load(Ordering::Acquire));
        assert_eq!(state.retired.load(Ordering::Acquire), 0);
        assert!(rx.try_recv().is_ok());
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Disconnected)));
    }
}
