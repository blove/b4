use super::{Error, Result};
use serde::Serialize;
use serde_json::Value;
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Component, Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
pub const NATIVE_LIMIT: usize = 24 * 1024 * 1024;
pub const CLIENT_LIMIT: usize = 8 * 1024 * 1024;
pub struct Quota {
    limit: usize,
    used: AtomicUsize,
}
impl Quota {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
        }
    }
    pub fn reserve(&self, next: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(next).filter(|total| *total <= self.limit)
            })
            .map(|_| ())
            .map_err(|_| Error("evidence_quota_exceeded"))
    }
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }
}
#[derive(Clone)]
pub struct Evidence {
    root: PathBuf,
    quota: Arc<Quota>,
}
impl Evidence {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            quota: Arc::new(Quota::new(NATIVE_LIMIT)),
        }
    }
    pub fn write(&self, name: &str, bytes: &[u8]) -> Result<()> {
        let mut components = Path::new(name).components();
        if !matches!(components.next(), Some(Component::Normal(_))) || components.next().is_some() {
            return Err(Error("evidence_invalid_name"));
        }
        self.quota.reserve(bytes.len())?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.root.join(name))
            .map_err(|_| Error("evidence_create_failed"))?;
        file.write_all(bytes)
            .map_err(|_| Error("evidence_write_failed"))?;
        verify_file(&self.root.join(name))?;
        Ok(())
    }
    pub fn json<T: Serialize>(&self, name: &str, value: &T) -> Result<()> {
        self.write(name, &encode(value, NATIVE_LIMIT)?)
    }
    pub fn finish(&self) -> Result<usize> {
        let current = self.quota.used();
        let bytes = ledger_bytes(current, NATIVE_LIMIT)?;
        self.write("quota-ledger.json", &bytes)?;
        let actual = account(&self.root)?;
        if actual != self.quota.used() {
            return Err(Error("evidence_accounting_mismatch"));
        }
        Ok(actual)
    }
}
struct Bounded(Vec<u8>, usize);
impl Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(io::Error::other("bounded_encoding_exceeded"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let mut output = Bounded(Vec::new(), limit);
    serde_json::to_writer(&mut output, value).map_err(|_| Error("evidence_encoding_failed"))?;
    output
        .write_all(b"\n")
        .map_err(|_| Error("evidence_encoding_failed"))?;
    Ok(output.0)
}
fn ledger_bytes(current: usize, limit: usize) -> Result<Vec<u8>> {
    let mut used = current;
    for _ in 0..16 {
        let bytes = encode(
            &serde_json::json!({"version":1,"limit_bytes":limit,"used_bytes":used}),
            8192,
        )?;
        let next = current
            .checked_add(bytes.len())
            .ok_or(Error("evidence_quota_exceeded"))?;
        if next == used {
            return Ok(bytes);
        }
        used = next;
    }
    Err(Error("evidence_encoding_failed"))
}
pub fn private_dir(path: &Path) -> Result<()> {
    fs::DirBuilder::new()
        .mode(0o700)
        .create(path)
        .map_err(|_| Error("private_directory_failed"))?;
    verify_dir(path)
}
pub fn verify_dir(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|_| Error("private_directory_failed"))?;
    if !m.is_dir() || m.permissions().mode() & 0o7777 != 0o700 {
        return Err(Error("private_directory_mode"));
    }
    Ok(())
}
pub fn verify_file(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|_| Error("private_file_failed"))?;
    if !m.is_file() || m.permissions().mode() & 0o7777 != 0o600 {
        return Err(Error("private_file_mode"));
    }
    Ok(())
}
pub fn prepare_store(path: &Path) -> Result<()> {
    private_dir(path)?;
    for name in ["owner.lock", "runtime.sqlite3"] {
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path.join(name))
            .map_err(|_| Error("private_store_create"))?;
    }
    verify_store(path)
}
pub fn verify_store(path: &Path) -> Result<()> {
    verify_dir(path)?;
    for entry in fs::read_dir(path).map_err(|_| Error("private_store_read"))? {
        let entry = entry.map_err(|_| Error("private_store_read"))?;
        let name = entry.file_name();
        if ![
            "owner.lock",
            "runtime.sqlite3",
            "runtime.sqlite3-journal",
            "runtime.sqlite3-wal",
            "runtime.sqlite3-shm",
        ]
        .iter()
        .any(|allowed| name == *allowed)
        {
            return Err(Error("private_store_unexpected_file"));
        }
        verify_file(&entry.path())?;
    }
    for name in ["owner.lock", "runtime.sqlite3"] {
        verify_file(&path.join(name))?;
    }
    Ok(())
}
pub fn account(path: &Path) -> Result<usize> {
    verify_dir(path)?;
    let mut total = 0usize;
    for entry in fs::read_dir(path).map_err(|_| Error("evidence_accounting_failed"))? {
        let path = entry
            .map_err(|_| Error("evidence_accounting_failed"))?
            .path();
        verify_file(&path)?;
        let size = usize::try_from(
            fs::metadata(path)
                .map_err(|_| Error("evidence_accounting_failed"))?
                .len(),
        )
        .map_err(|_| Error("evidence_accounting_failed"))?;
        total = total
            .checked_add(size)
            .ok_or(Error("evidence_accounting_failed"))?;
    }
    Ok(total)
}
pub fn verify_client(path: &Path) -> Result<usize> {
    let total = account(path)?;
    if total > CLIENT_LIMIT {
        return Err(Error("client_quota_exceeded"));
    }
    let names: Vec<_> = fs::read_dir(path)
        .map_err(|_| Error("client_ledger_invalid"))?
        .map(|entry| entry.map(|e| e.file_name()))
        .collect::<std::result::Result<_, _>>()
        .map_err(|_| Error("client_ledger_invalid"))?;
    if names.len() != 2
        || !names
            .iter()
            .all(|n| n == "client-observations.json" || n == "quota-ledger.json")
    {
        return Err(Error("client_evidence_invalid"));
    }
    let file = std::fs::File::open(path.join("quota-ledger.json"))
        .map_err(|_| Error("client_ledger_missing"))?;
    let mut ledger = Vec::new();
    file.take(8193)
        .read_to_end(&mut ledger)
        .map_err(|_| Error("client_ledger_invalid"))?;
    if ledger.len() > 8192 {
        return Err(Error("client_ledger_invalid"));
    }
    let value: Value =
        serde_json::from_slice(&ledger).map_err(|_| Error("client_ledger_invalid"))?;
    if value.as_object().is_none_or(|map| map.len() != 3)
        || value["version"] != 1
        || value["limit_bytes"] != CLIENT_LIMIT
        || value["used_bytes"] != total
    {
        return Err(Error("client_ledger_invalid"));
    }
    Ok(total)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exhausted_native_quota_refuses_before_write() {
        let native = Quota::new(NATIVE_LIMIT);
        let client = Quota::new(CLIENT_LIMIT);
        native.reserve(NATIVE_LIMIT).unwrap();
        client.reserve(1).unwrap();
        assert!(native.reserve(1).is_err());
        assert_eq!(native.used(), NATIVE_LIMIT);
        assert_eq!(client.used(), 1);
        client.reserve(CLIENT_LIMIT - 1).unwrap();
        assert!(client.reserve(1).is_err());
        assert_eq!(client.used(), CLIENT_LIMIT);
    }
    #[test]
    fn ledger_accounts_its_own_encoded_bytes() {
        for current in [0, 8, 99, 999, 9999, NATIVE_LIMIT - 100] {
            let bytes = ledger_bytes(current, NATIVE_LIMIT).unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["used_bytes"], current + bytes.len());
        }
    }
    #[test]
    fn bounded_encoding_refuses_before_excess_bytes() {
        assert!(encode(&"123456789", 4).is_err());
        assert_eq!(encode(&1, 2).unwrap(), b"1\n");
    }
    #[test]
    fn failed_existing_file_write_consumes_reservation_and_preserves_bytes() {
        let path = std::env::temp_dir().join(format!("b4-ap-quota-{}", uuid::Uuid::new_v4()));
        private_dir(&path).unwrap();
        let evidence = Evidence {
            root: path.clone(),
            quota: Arc::new(Quota::new(4)),
        };
        evidence.write("one", b"abc").unwrap();
        assert!(evidence.write("one", b"x").is_err());
        assert_eq!(fs::read(path.join("one")).unwrap(), b"abc");
        assert_eq!(evidence.quota.used(), 4);
        assert!(evidence.write("two", b"x").is_err());
        assert!(!path.join("two").exists());
        fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn ledger_reservation_cannot_borrow_client_capacity_or_exceed_native_limit() {
        let path = std::env::temp_dir().join(format!("b4-ap-ledger-cap-{}", uuid::Uuid::new_v4()));
        private_dir(&path).unwrap();
        let evidence = Evidence {
            root: path.clone(),
            quota: Arc::new(Quota::new(3)),
        };
        evidence.write("data", b"abc").unwrap();
        assert!(evidence.finish().is_err());
        assert!(!path.join("quota-ledger.json").exists());
        assert_eq!(account(&path).unwrap(), 3);
        fs::remove_dir_all(path).unwrap();
    }
}
#[cfg(test)]
mod privacy_tests {
    use super::*;
    #[test]
    fn real_sqlite_files_are_reserved_private_and_foreign_files_are_preserved() {
        use b4_runtime::Store;
        let path = std::env::temp_dir().join(format!("b4-ap-modes-{}", uuid::Uuid::new_v4()));
        prepare_store(&path).unwrap();
        let store = b4_sqlite::SqliteStore::open(&path).unwrap();
        verify_store(&path).unwrap();
        assert!(store.readiness().is_ok());
        drop(store);
        verify_store(&path).unwrap();
        let original = std::fs::read(path.join("owner.lock")).unwrap();
        assert!(prepare_store(&path).is_err());
        assert_eq!(std::fs::read(path.join("owner.lock")).unwrap(), original);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn flat_evidence_and_exact_modes_refuse_escaping_names_links_and_special_bits() {
        let path =
            std::env::temp_dir().join(format!("b4-ap-private-refusal-{}", uuid::Uuid::new_v4()));
        private_dir(&path).unwrap();
        let evidence = Evidence::new(path.clone());
        for name in ["/", ".", "..", "../outside", "nested/file", ""] {
            assert!(evidence.write(name, b"x").is_err());
        }
        assert_eq!(evidence.quota.used(), 0);
        evidence.write("private", b"one").unwrap();
        std::os::unix::fs::symlink(path.join("private"), path.join("link")).unwrap();
        assert!(verify_file(&path.join("link")).is_err());
        fs::set_permissions(path.join("private"), fs::Permissions::from_mode(0o1600)).unwrap();
        assert!(verify_file(&path.join("private")).is_err());
        fs::set_permissions(path.join("private"), fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o1700)).unwrap();
        assert!(verify_dir(&path).is_err());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        fs::remove_dir_all(path).unwrap();
    }
}
