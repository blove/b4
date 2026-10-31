use super::{model::*, THREAD};
use b4_contract::ThreadId;
use b4_runtime::{approval::ApprovalStore, Store};
use b4_sqlite::SqliteStore;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, DirBuilder, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

pub const CAPTURE_BYTES: usize = 1_048_576;
pub const EFFECT_BYTES: usize = 1024;
pub const AGGREGATE_BYTES: usize = 32 * 1_048_576;
// Reserve the fixed external-ledger maximum inside the run-wide cap.
pub const PARENT_BYTES: usize = 16 * 1_048_576 - EFFECT_BYTES;
pub const CHILD_BYTES: usize = 8 * 1_048_576;
pub const LOG_BYTES: usize = 1_048_576;

pub fn now_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Failure::Setup)?
            .as_millis(),
    )
    .map_err(|_| Failure::Bound)
}
pub fn directory(path: &Path) -> Result<()> {
    DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|_| Failure::Io)?;
    verify_directory(path)
}
pub fn verify_directory(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|_| Failure::Io)?;
    if m.is_dir() && !m.file_type().is_symlink() && m.permissions().mode() & 0o777 == 0o700 {
        Ok(())
    } else {
        Err(Failure::Permission)
    }
}
pub fn new_file(path: &Path) -> Result<std::fs::File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| Failure::Io)
}
pub fn verify_file(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|_| Failure::Io)?;
    if m.is_file() && !m.file_type().is_symlink() && m.permissions().mode() & 0o777 == 0o600 {
        Ok(())
    } else {
        Err(Failure::Permission)
    }
}
pub fn verify_store(root: &Path) -> Result<()> {
    let data = root.join("data");
    verify_directory(&data)?;
    for name in ["owner.lock", "runtime.sqlite3"] {
        verify_file(&data.join(name))?;
    }
    for name in [
        "runtime.sqlite3-wal",
        "runtime.sqlite3-shm",
        "runtime.sqlite3-journal",
    ] {
        let p = data.join(name);
        match fs::symlink_metadata(&p) {
            Ok(_) => verify_file(&p)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(Failure::Io),
        }
    }
    Ok(())
}

/// The new fixture is isolated and small. Old Store collections are trusted
/// finite fixture loads; this is not a new bound-before-collection Store port.
pub fn capture(store: &SqliteStore, lane: Lane, role: Role, counts: Counts) -> Result<Capture> {
    store.readiness().map_err(|_| Failure::Setup)?;
    let ordinary = store
        .thread_view(&ThreadId::new(THREAD).map_err(|_| Failure::Setup)?)
        .map_err(|_| Failure::Setup)?
        .ok_or(Failure::Setup)?;
    let execution = ordinary.execution.as_ref().ok_or(Failure::Setup)?;
    let authority = store
        .approval_view(&execution.snapshot.execution_id)
        .map_err(|_| Failure::Setup)?
        .ok_or(Failure::Setup)?;
    let journal = store
        .journal(&execution.snapshot.execution_id)
        .map_err(|_| Failure::Setup)?;
    let c = Capture {
        version: 1,
        lane,
        role,
        owner: store.owner_generation(),
        ordinary,
        authority: Authority {
            thread: authority.thread,
            execution: authority.execution,
            grants: authority.grants,
            acceptances: authority.acceptances,
        },
        journal,
        counts,
    };
    if !c.coherent() {
        return Err(Failure::Bound);
    }
    Ok(c)
}

struct Capped {
    bytes: Vec<u8>,
    cap: usize,
}
impl Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.cap)
        {
            return Err(std::io::Error::other("private_serialization_limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub fn encode<T: Serialize>(value: &T, cap: usize) -> Result<Vec<u8>> {
    let mut out = Capped {
        bytes: Vec::new(),
        cap,
    };
    serde_json::to_writer(&mut out, value).map_err(|_| Failure::Bound)?;
    Ok(out.bytes)
}
pub fn read_json<T: DeserializeOwned>(path: &Path, cap: usize) -> Result<T> {
    verify_file(path)?;
    let m = fs::metadata(path).map_err(|_| Failure::Io)?;
    if m.len() > u64::try_from(cap).map_err(|_| Failure::Bound)? {
        return Err(Failure::Bound);
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| Failure::Io)?
        .take(
            u64::try_from(cap)
                .map_err(|_| Failure::Bound)?
                .checked_add(1)
                .ok_or(Failure::Bound)?,
        )
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::Io)?;
    if bytes.len() > cap {
        return Err(Failure::Bound);
    }
    serde_json::from_slice(&bytes).map_err(|_| Failure::Protocol)
}

#[derive(Clone)]
pub struct Writer {
    path: PathBuf,
    state: Arc<Mutex<Budget>>,
}
struct Budget {
    limit: usize,
    used: usize,
    sealed: bool,
    loss: bool,
}
#[derive(Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    version: u8,
    limit_bytes: usize,
    used_bytes: usize,
}
impl Writer {
    pub fn new(path: PathBuf, limit: usize) -> Result<Self> {
        verify_directory(&path)?;
        Ok(Self {
            path,
            state: Arc::new(Mutex::new(Budget {
                limit,
                used: 0,
                sealed: false,
                loss: false,
            })),
        })
    }
    fn reserve(&self, n: usize) -> Result<()> {
        let mut s = self.state.lock().map_err(|_| Failure::Io)?;
        if s.sealed || s.loss || s.used.checked_add(n).is_none_or(|x| x > s.limit) {
            s.loss = true;
            return Err(Failure::Bound);
        }
        s.used = s.used.checked_add(n).ok_or(Failure::Bound)?;
        Ok(())
    }
    pub fn mark_loss(&self) {
        if let Ok(mut s) = self.state.lock() {
            s.loss = true;
        }
    }
    pub fn persist<T: Serialize>(&self, name: &str, value: &T, cap: usize) -> Result<()> {
        let bytes = encode(value, cap)?;
        self.persist_bytes(name, &bytes)
    }
    pub fn persist_bytes(&self, name: &str, bytes: &[u8]) -> Result<()> {
        if name.contains('/') || name.contains('\\') || name.starts_with('.') || name.is_empty() {
            return Err(Failure::Protocol);
        }
        self.reserve(bytes.len())?;
        let mut f = new_file(&self.path.join(name))?;
        f.write_all(bytes).map_err(|_| Failure::Io)?;
        f.sync_all().map_err(|_| Failure::Io)?;
        verify_file(&self.path.join(name))
    }
    /// Diagnostic readers reserve every persisted byte before each actual write.
    pub fn open_log(&self, name: &str) -> Result<std::fs::File> {
        if !matches!(
            name,
            "seed-stdout.log" | "seed-stderr.log" | "restart-stdout.log" | "restart-stderr.log"
        ) {
            return Err(Failure::Protocol);
        }
        new_file(&self.path.join(name))
    }
    pub fn log_bytes(&self, file: &mut std::fs::File, bytes: &[u8]) -> Result<()> {
        self.reserve(bytes.len())?;
        file.write_all(bytes).map_err(|_| Failure::Io)
    }
    pub fn seal(&self) -> Result<()> {
        let mut s = self.state.lock().map_err(|_| Failure::Io)?;
        if s.sealed || s.loss {
            return Err(Failure::Bound);
        }
        let base = s.used;
        let mut total = base;
        let mut encoded = None;
        for _ in 0..32 {
            let bytes = encode(
                &Ledger {
                    version: 1,
                    limit_bytes: s.limit,
                    used_bytes: total,
                },
                256,
            )?;
            let next = base.checked_add(bytes.len()).ok_or(Failure::Bound)?;
            if next > s.limit {
                return Err(Failure::Bound);
            }
            if next == total {
                encoded = Some(bytes);
                break;
            }
            total = next;
        }
        let bytes = encoded.ok_or(Failure::Bound)?;
        // This fixed-point loop increases by at most the decimal width of usize;
        // each iteration serializes only a fixed three-field, <=256-byte DTO.
        s.used = total;
        s.sealed = true;
        let mut f = new_file(&self.path.join("quota-ledger.json"))?;
        f.write_all(&bytes).map_err(|_| Failure::Io)?;
        f.sync_all().map_err(|_| Failure::Io)?;
        let limit = s.limit;
        drop(s);
        Self::verify_sealed(&self.path, limit)
    }
    pub fn verify_sealed(path: &Path, expected_limit: usize) -> Result<()> {
        Self::sealed_bytes(path, expected_limit).map(|_| ())
    }
    fn sealed_bytes(path: &Path, expected_limit: usize) -> Result<usize> {
        verify_directory(path)?;
        let ledger: Ledger = read_json(&path.join("quota-ledger.json"), 256)?;
        let mut sum = 0usize;
        for entry in fs::read_dir(path).map_err(|_| Failure::Io)? {
            let p = entry.map_err(|_| Failure::Io)?.path();
            verify_file(&p)?;
            sum = sum
                .checked_add(
                    usize::try_from(fs::metadata(p).map_err(|_| Failure::Io)?.len())
                        .map_err(|_| Failure::Bound)?,
                )
                .ok_or(Failure::Bound)?;
        }
        if ledger.version != 1
            || ledger.limit_bytes != expected_limit
            || sum != ledger.used_bytes
            || sum > ledger.limit_bytes
        {
            return Err(Failure::Bound);
        }
        Ok(sum)
    }
}

/// Independently sum every role's actual sealed files (including quota ledgers)
/// and the actual external ledger. SQLite data/sidecars remain outside evidence.
pub fn verify_aggregate(root: &Path) -> Result<()> {
    let mut sum = 0usize;
    for (role, limit) in [
        ("parent", PARENT_BYTES),
        ("seed", CHILD_BYTES),
        ("restart", CHILD_BYTES),
    ] {
        sum = sum
            .checked_add(Writer::sealed_bytes(
                &root.join("evidence").join(role),
                limit,
            )?)
            .ok_or(Failure::Bound)?;
    }
    let effects = root.join("effects.ledger");
    verify_file(&effects)?;
    let length = usize::try_from(fs::metadata(effects).map_err(|_| Failure::Io)?.len())
        .map_err(|_| Failure::Bound)?;
    if length > EFFECT_BYTES {
        return Err(Failure::Bound);
    }
    sum = sum.checked_add(length).ok_or(Failure::Bound)?;
    if sum > AGGREGATE_BYTES {
        return Err(Failure::Bound);
    }
    Ok(())
}

pub struct Directory {
    pub root: PathBuf,
    pub parent: Writer,
}
impl Directory {
    pub fn new() -> Result<Self> {
        let base = if cfg!(target_os = "macos") {
            Path::new("/private/tmp")
        } else {
            Path::new("/tmp")
        };
        let root = base.join(format!("b4sr-{}", uuid::Uuid::new_v4().simple()));
        directory(&root)?;
        directory(&root.join("data"))?;
        // Ordinary Store uses ambient creation modes: precreate its exact paths.
        for name in ["owner.lock", "runtime.sqlite3"] {
            new_file(&root.join("data").join(name))?
                .sync_all()
                .map_err(|_| Failure::Io)?;
        }
        new_file(&root.join("effects.ledger"))?
            .sync_all()
            .map_err(|_| Failure::Io)?;
        directory(&root.join("evidence"))?;
        for name in ["parent", "seed", "restart"] {
            directory(&root.join("evidence").join(name))?;
        }
        let parent = Writer::new(root.join("evidence/parent"), PARENT_BYTES)?;
        verify_store(&root)?;
        Ok(Self { root, parent })
    }
    // Evidence is retained on success and failure. This guard never deletes
    // someone else's directory or destroys the real crash/restart observations.
}
pub fn ledger(root: &Path) -> Result<Vec<String>> {
    let path = root.join("effects.ledger");
    verify_file(&path)?;
    if fs::metadata(&path).map_err(|_| Failure::Io)?.len() > EFFECT_BYTES as u64 {
        return Err(Failure::Bound);
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&path)
        .map_err(|_| Failure::Io)?
        .take((EFFECT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::Io)?;
    if bytes.len() > EFFECT_BYTES {
        return Err(Failure::Bound);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| Failure::Protocol)?;
    let values: Vec<_> = text.lines().map(str::to_owned).collect();
    if values.len() > 2
        || values
            .iter()
            .any(|v| !matches!(v.as_str(), "sibling" | "release" | "unsafe"))
    {
        return Err(Failure::Protocol);
    }
    Ok(values)
}
