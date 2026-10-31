use super::InspectionError;
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use std::fs::{File, Metadata, OpenOptions};
use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
use std::path::{Path, PathBuf};

const MAX_PATH: usize = 4096;
const MAX_SOURCE: u64 = 64 * 1024 * 1024;
const SIDECARS: [&str; 3] = [
    "runtime.sqlite3-wal",
    "runtime.sqlite3-shm",
    "runtime.sqlite3-journal",
];

#[derive(PartialEq, Eq)]
struct Witness {
    device: u64,
    inode: u64,
    mode: u32,
    size: u64,
    modified: i64,
    modified_ns: i64,
}

impl From<Metadata> for Witness {
    fn from(value: Metadata) -> Self {
        Self {
            device: value.dev(),
            inode: value.ino(),
            mode: value.mode(),
            size: value.len(),
            modified: value.mtime(),
            modified_ns: value.mtime_nsec(),
        }
    }
}

struct Source {
    directory: PathBuf,
    witnesses: [Witness; 3],
}

fn bounded_path(path: &Path) -> Result<(), InspectionError> {
    let bytes = path.as_os_str().as_bytes();
    if bytes.len() > MAX_PATH {
        return Err(InspectionError::LimitExceeded);
    }
    if bytes.contains(&0) {
        return Err(InspectionError::InvalidSource);
    }
    Ok(())
}

fn metadata(path: &Path, directory: bool) -> Result<Witness, InspectionError> {
    let value = std::fs::symlink_metadata(path).map_err(|_| InspectionError::InvalidSource)?;
    if (directory && (!value.is_dir() || value.mode() & 0o7777 != 0o700))
        || (!directory && (!value.is_file() || value.mode() & 0o7777 != 0o600))
    {
        return Err(InspectionError::InvalidSource);
    }
    Ok(value.into())
}

impl Source {
    fn preflight(path: &Path) -> Result<Self, InspectionError> {
        bounded_path(path)?;
        // Check the supplied root itself before resolving its trusted parent path.
        let supplied: PathBuf = path.components().collect();
        let root = metadata(&supplied, true)?;
        let directory =
            std::fs::canonicalize(&supplied).map_err(|_| InspectionError::InvalidSource)?;
        bounded_path(&directory)?;
        bounded_path(&directory.join("runtime.sqlite3"))?;
        let database = metadata(&directory.join("runtime.sqlite3"), false)?;
        let lock = metadata(&directory.join("owner.lock"), false)?;
        if database.size == 0 {
            return Err(InspectionError::InvalidSource);
        }
        if database.size > MAX_SOURCE {
            return Err(InspectionError::LimitExceeded);
        }
        let source = Self {
            directory,
            witnesses: [root, database, lock],
        };
        source.recheck()?;
        Ok(source)
    }

    fn recheck(&self) -> Result<(), InspectionError> {
        let actual = [
            metadata(&self.directory, true)?,
            metadata(&self.directory.join("runtime.sqlite3"), false)?,
            metadata(&self.directory.join("owner.lock"), false)?,
        ];
        if actual != self.witnesses {
            return Err(InspectionError::InvalidSource);
        }
        Ok(())
    }

    fn no_sidecars(&self) -> Result<(), InspectionError> {
        for name in SIDECARS {
            match std::fs::symlink_metadata(self.directory.join(name)) {
                Ok(_) => return Err(InspectionError::SidecarsPresent),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(InspectionError::Unavailable),
            }
        }
        Ok(())
    }

    fn uri(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let path = self.directory.join("runtime.sqlite3");
        let mut uri = String::with_capacity(path.as_os_str().as_bytes().len() * 3 + 32);
        uri.push_str("file:");
        for byte in path.as_os_str().as_bytes() {
            if byte.is_ascii_alphanumeric() || matches!(*byte, b'/' | b'-' | b'_' | b'.' | b'~') {
                uri.push(char::from(*byte));
            } else {
                uri.push('%');
                uri.push(char::from(HEX[usize::from(*byte >> 4)]));
                uri.push(char::from(HEX[usize::from(*byte & 15)]));
            }
        }
        uri.push_str("?mode=ro&immutable=1");
        uri
    }
}

/// Owns physical retirement ordering. A failed close retains this exact
/// connection and locked file description together until process teardown.
pub(super) struct Reader {
    connection: Option<Connection>,
    expected_connection: Option<Connection>,
    lock: Option<File>,
    source: Source,
    retirement_uncertain: bool,
}

struct Quarantine {
    _connection: Option<Connection>,
    _expected_connection: Option<Connection>,
    _lock: File,
}

impl Reader {
    pub(super) fn open(path: &Path) -> Result<Self, InspectionError> {
        let source = Source::preflight(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .open(source.directory.join("owner.lock"))
            .map_err(|_| InspectionError::InvalidSource)?;
        if Witness::from(lock.metadata().map_err(|_| InspectionError::Unavailable)?)
            != source.witnesses[2]
        {
            return Err(InspectionError::InvalidSource);
        }
        lock.try_lock_exclusive().map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                InspectionError::AlreadyOwned
            } else {
                InspectionError::Unavailable
            }
        })?;
        let mut reader = Self {
            connection: None,
            expected_connection: None,
            lock: Some(lock),
            source,
            retirement_uncertain: false,
        };
        let opened = (|| {
            reader.source.recheck()?;
            reader.source.no_sidecars()?;
            let flags = OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_URI
                | OpenFlags::SQLITE_OPEN_NOFOLLOW
                | OpenFlags::SQLITE_OPEN_PRIVATE_CACHE;
            reader.connection = Some(
                Connection::open_with_flags(reader.source.uri(), flags)
                    .map_err(|_| InspectionError::CorruptSource)?,
            );
            if !reader
                .connection()
                .is_readonly(rusqlite::MAIN_DB)
                .map_err(|_| InspectionError::Unavailable)?
            {
                return Err(InspectionError::Unavailable);
            }
            Ok(())
        })();
        if let Err(error) = opened {
            reader.finish()?;
            return Err(error);
        }
        Ok(reader)
    }

    pub(super) fn connection(&self) -> &Connection {
        // The only callers run between successful open and consuming finish.
        self.connection
            .as_ref()
            .expect("inspection reader owns its connection")
    }

    fn quarantine(&mut self, connection: Option<Connection>) {
        self.retirement_uncertain = true;
        if let Some(lock) = self.lock.take() {
            let _ = Box::leak(Box::new(Quarantine {
                _connection: connection.or_else(|| self.connection.take()),
                _expected_connection: self.expected_connection.take(),
                _lock: lock,
            }));
        }
    }

    fn close(&mut self) -> Result<(), InspectionError> {
        self.close_expected()?;
        if let Some(connection) = self.connection.take() {
            if let Err((connection, _)) = connection.close() {
                self.quarantine(Some(connection));
                return Err(InspectionError::CloseUncertain);
            }
        }
        Ok(())
    }

    pub(super) fn open_expected(&mut self) -> Result<(), InspectionError> {
        self.expected_connection =
            Some(Connection::open_in_memory().map_err(|_| InspectionError::Unavailable)?);
        Ok(())
    }

    pub(super) fn expected(&self) -> &Connection {
        self.expected_connection
            .as_ref()
            .expect("inspection guard owns expected schema connection")
    }

    pub(super) fn close_expected(&mut self) -> Result<(), InspectionError> {
        if let Some(connection) = self.expected_connection.take() {
            if let Err((connection, _)) = connection.close() {
                self.expected_connection = Some(connection);
                self.quarantine(None);
                return Err(InspectionError::CloseUncertain);
            }
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<(), InspectionError> {
        if self.retirement_uncertain {
            return Err(InspectionError::CloseUncertain);
        }
        let before = self
            .source
            .recheck()
            .and_then(|()| self.source.no_sidecars());
        self.close()?;
        let checked = self
            .source
            .recheck()
            .and_then(|()| self.source.no_sidecars());
        self.unlock()?;
        before?;
        checked
    }

    fn unlock(&mut self) -> Result<(), InspectionError> {
        if let Some(lock) = self.lock.as_ref() {
            if FileExt::unlock(lock).is_err() {
                self.quarantine(None);
                return Err(InspectionError::CloseUncertain);
            }
        }
        self.lock.take();
        Ok(())
    }
}

#[cfg(test)]
mod tests;

impl Drop for Reader {
    fn drop(&mut self) {
        if self.close().is_ok() {
            let _ = self.unlock();
        }
    }
}
