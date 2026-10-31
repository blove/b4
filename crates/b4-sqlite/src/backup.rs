use crate::SqliteStore;
use std::{fmt, path::Path};

/// Static backup failures never retain private paths or database contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackupError {
    InvalidDestination,
    DestinationExists,
    InvalidDatabase,
    Unavailable,
    PublicationUncertain,
    UnsupportedPlatform,
}
impl fmt::Display for BackupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidDestination => "backup_destination_invalid",
            Self::DestinationExists => "backup_destination_exists",
            Self::InvalidDatabase => "backup_database_invalid",
            Self::Unavailable => "backup_unavailable",
            Self::PublicationUncertain => "backup_publication_uncertain",
            Self::UnsupportedPlatform => "backup_platform_unsupported",
        })
    }
}
impl std::error::Error for BackupError {}

impl SqliteStore {
    /// Export a private, standalone archive without replacing any destination.
    ///
    /// This synchronous operation has no work deadline or disk/size budget.
    /// Before controlled restoration, the embedding must stop all writers,
    /// quiesce owned work and retire the source. An archive cannot fence another
    /// directory or undo external effects. After `PublicationUncertain`, a valid
    /// final file may already exist; callers must not retry by overwriting it.
    pub fn export_backup(&mut self, destination: impl AsRef<Path>) -> Result<(), BackupError> {
        #[cfg(unix)]
        {
            unix::export(self, destination.as_ref(), &unix::Filesystem)
        }
        #[cfg(not(unix))]
        {
            let _ = destination;
            Err(BackupError::UnsupportedPlatform)
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use crate::{
        checksum, decode_u64, read_metadata, verify_schema, LEGACY_SCHEMA_VERSION, SCHEMA,
        SCHEMA_V3, SCHEMA_VERSION,
    };
    use b4_runtime::{generate_thread_generation, StoreError};
    use rusqlite::{Connection, OpenFlags};
    use std::{
        fs::{File, OpenOptions, Permissions},
        io,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
        path::PathBuf,
    };

    const MAX_STAGE_ATTEMPTS: usize = 8;

    pub(super) trait PublicationIo {
        fn sync_stage(&self, stage: &File) -> io::Result<()>;
        fn link(&self, stage: &Path, destination: &Path) -> io::Result<()>;
        fn remove_stage(&self, stage: &Path) -> io::Result<()>;
        fn sync_parent(&self, parent: &Path) -> io::Result<()>;
    }
    pub(super) struct Filesystem;
    impl PublicationIo for Filesystem {
        fn sync_stage(&self, stage: &File) -> io::Result<()> {
            stage.sync_all()
        }
        fn link(&self, stage: &Path, destination: &Path) -> io::Result<()> {
            std::fs::hard_link(stage, destination)
        }
        fn remove_stage(&self, stage: &Path) -> io::Result<()> {
            std::fs::remove_file(stage)
        }
        fn sync_parent(&self, parent: &Path) -> io::Result<()> {
            File::open(parent)?.sync_all()
        }
    }
    pub(super) fn export(
        store: &mut SqliteStore,
        requested: &Path,
        io: &impl PublicationIo,
    ) -> Result<(), BackupError> {
        let destination = destination(&store.data_dir, requested)?;
        let parent = destination
            .parent()
            .ok_or(BackupError::InvalidDestination)?;
        let owner = store.owner_generation;
        let connection = store
            .connection
            .get_mut()
            .map_err(|_| BackupError::Unavailable)?;
        validate_database(connection, owner)?;
        let (stage, file) = reserve_stage(parent)?;
        let mut published = false;
        let result = (|| {
            let name = stage.to_str().ok_or(BackupError::InvalidDestination)?;
            connection
                .execute("VACUUM main INTO ?1", [name])
                .map_err(|_| BackupError::Unavailable)?;
            validate_archive(&stage, owner)?;
            io.sync_stage(&file).map_err(|_| BackupError::Unavailable)?;
            io.link(&stage, &destination).map_err(|error| {
                if error.kind() == io::ErrorKind::AlreadyExists {
                    BackupError::DestinationExists
                } else {
                    BackupError::Unavailable
                }
            })?;
            published = true;
            io.remove_stage(&stage)
                .map_err(|_| BackupError::PublicationUncertain)?;
            io.sync_parent(parent)
                .map_err(|_| BackupError::PublicationUncertain)?;
            Ok(())
        })();
        if result.is_err() && !published {
            cleanup_stage(&stage, io);
        }
        result
    }

    fn destination(source: &Path, requested: &Path) -> Result<PathBuf, BackupError> {
        let text = requested.to_str().ok_or(BackupError::InvalidDestination)?;
        if text.contains('\0') || matches!(text.rsplit('/').next(), Some("" | "." | "..") | None) {
            return Err(BackupError::InvalidDestination);
        }
        let filename = requested
            .file_name()
            .ok_or(BackupError::InvalidDestination)?;
        let parent = requested
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = std::fs::canonicalize(parent).map_err(|_| BackupError::InvalidDestination)?;
        if !parent.is_dir() || parent.starts_with(source) {
            return Err(BackupError::InvalidDestination);
        }
        let destination = parent.join(filename);
        destination
            .to_str()
            .ok_or(BackupError::InvalidDestination)?;
        match std::fs::symlink_metadata(&destination) {
            Ok(_) => Err(BackupError::DestinationExists),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(destination),
            Err(_) => Err(BackupError::Unavailable),
        }
    }

    fn reserve_stage(parent: &Path) -> Result<(PathBuf, File), BackupError> {
        for _ in 0..MAX_STAGE_ATTEMPTS {
            let nonce = generate_thread_generation().map_err(|_| BackupError::Unavailable)?;
            let stage = parent.join(format!(".b4-backup-{}.sqlite3", nonce.as_str()));
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&stage)
            {
                Ok(file) => {
                    // Creation is private even with umask; establish owner RW
                    // explicitly if that umask also removed owner permissions.
                    if file.set_permissions(Permissions::from_mode(0o600)).is_err() {
                        let _ = std::fs::remove_file(&stage);
                        return Err(BackupError::Unavailable);
                    }
                    return Ok((stage, file));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(BackupError::Unavailable),
            }
        }
        Err(BackupError::Unavailable)
    }

    fn validate_database(connection: &Connection, expected_owner: u64) -> Result<(), BackupError> {
        let (version, stored_checksum, owner) =
            read_metadata(connection).map_err(|_| BackupError::InvalidDatabase)?;
        let expected_checksum = match version {
            LEGACY_SCHEMA_VERSION => checksum(SCHEMA_V3),
            SCHEMA_VERSION => checksum(SCHEMA),
            _ => return Err(BackupError::InvalidDatabase),
        };
        if stored_checksum != expected_checksum
            || expected_owner == 0
            || decode_u64(&owner).map_err(|_| BackupError::InvalidDatabase)? != expected_owner
        {
            return Err(BackupError::InvalidDatabase);
        }
        verify_schema(connection, version).map_err(|error| {
            if matches!(error, StoreError::Unavailable(_)) {
                BackupError::Unavailable
            } else {
                BackupError::InvalidDatabase
            }
        })?;
        if version == SCHEMA_VERSION {
            // Nonempty permission records require their separate bounded validator.
            let has_permission_records: bool = connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM permission_acceptances LIMIT 1) \
                     OR EXISTS(SELECT 1 FROM permission_decisions LIMIT 1) \
                     OR EXISTS(SELECT 1 FROM permission_uses LIMIT 1)",
                    [],
                    |row| row.get(0),
                )
                .map_err(|_| BackupError::InvalidDatabase)?;
            if has_permission_records {
                return Err(BackupError::InvalidDatabase);
            }
        }
        Ok(())
    }

    fn validate_archive(stage: &Path, owner: u64) -> Result<(), BackupError> {
        // Explicit flags omit SQLITE_OPEN_URI and cannot create a missing file.
        let copied = Connection::open_with_flags(stage, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| BackupError::Unavailable)?;
        validate_database(&copied, owner)?;
        let journal: String = copied
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .map_err(|_| BackupError::InvalidDatabase)?;
        if journal != "delete" {
            return Err(BackupError::InvalidDatabase);
        }
        {
            let mut statement = copied
                .prepare("PRAGMA quick_check(1)")
                .map_err(|_| BackupError::InvalidDatabase)?;
            let mut rows = statement
                .query([])
                .map_err(|_| BackupError::InvalidDatabase)?;
            let row = rows
                .next()
                .map_err(|_| BackupError::InvalidDatabase)?
                .ok_or(BackupError::InvalidDatabase)?;
            let result: String = row.get(0).map_err(|_| BackupError::InvalidDatabase)?;
            if result != "ok"
                || rows
                    .next()
                    .map_err(|_| BackupError::InvalidDatabase)?
                    .is_some()
            {
                return Err(BackupError::InvalidDatabase);
            }
        }
        {
            let mut statement = copied
                .prepare("PRAGMA foreign_key_check")
                .map_err(|_| BackupError::InvalidDatabase)?;
            if statement
                .query([])
                .map_err(|_| BackupError::InvalidDatabase)?
                .next()
                .map_err(|_| BackupError::InvalidDatabase)?
                .is_some()
            {
                return Err(BackupError::InvalidDatabase);
            }
        }
        drop(copied);
        for sidecar in sidecars(stage) {
            match std::fs::symlink_metadata(sidecar) {
                Ok(_) => return Err(BackupError::InvalidDatabase),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(_) => return Err(BackupError::Unavailable),
            }
        }
        Ok(())
    }

    fn sidecars(stage: &Path) -> [PathBuf; 3] {
        ["-journal", "-wal", "-shm"].map(|suffix| {
            let mut name = stage.as_os_str().to_owned();
            name.push(suffix);
            PathBuf::from(name)
        })
    }

    fn cleanup_stage(stage: &Path, io: &impl PublicationIo) {
        // Only the uniquely reserved stage and its exact SQLite sidecars belong
        // to this operation; never remove a caller's final or collision files.
        for sidecar in sidecars(stage) {
            let _ = std::fs::remove_file(sidecar);
        }
        let _ = io.remove_stage(stage);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::unix::*;
    use super::*;
    use b4_runtime::Store;
    use rusqlite::{Connection, OpenFlags};
    use std::{
        cell::RefCell,
        fs::File,
        io,
        os::unix::fs::{DirBuilderExt, PermissionsExt},
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "b4-backup-fault-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .unwrap();
            Self(path)
        }
        fn store(&self) -> SqliteStore {
            SqliteStore::open(self.0.join("source")).unwrap()
        }
        fn archive(&self) -> PathBuf {
            self.0.join("archive.sqlite3")
        }
        fn stages(&self) -> Vec<PathBuf> {
            std::fs::read_dir(&self.0)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| {
                    path.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with(".b4-backup-")
                })
                .collect()
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    #[derive(Clone, Copy, PartialEq)]
    enum Failure {
        StageSync,
        SidecarsStageSync,
        Link,
        Remove,
        ParentSync,
        Race,
    }
    struct FaultIo {
        fail: Failure,
        events: RefCell<Vec<&'static str>>,
        parent: Option<PathBuf>,
    }
    impl FaultIo {
        fn new(fail: Failure) -> Self {
            Self {
                fail,
                events: RefCell::new(Vec::new()),
                parent: None,
            }
        }
        fn error() -> io::Error {
            io::Error::other("private filesystem diagnostic marker")
        }
    }
    impl PublicationIo for FaultIo {
        fn sync_stage(&self, stage: &File) -> io::Result<()> {
            self.events.borrow_mut().push("stage_sync");
            // A real database was written before the injected operation.
            assert!(stage.metadata().unwrap().len() > 0);
            if self.fail == Failure::SidecarsStageSync {
                use std::os::unix::fs::OpenOptionsExt;
                let stage_path = std::fs::read_dir(self.parent.as_ref().unwrap())
                    .unwrap()
                    .map(|entry| entry.unwrap().path())
                    .find(|path| {
                        path.file_name()
                            .unwrap()
                            .to_string_lossy()
                            .starts_with(".b4-backup-")
                            && path
                                .extension()
                                .is_some_and(|extension| extension == "sqlite3")
                    })
                    .unwrap();
                for suffix in ["-journal", "-wal", "-shm"] {
                    let mut path = stage_path.as_os_str().to_owned();
                    path.push(suffix);
                    let file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(PathBuf::from(path))
                        .unwrap();
                    file.sync_all().unwrap();
                }
                Err(Self::error())
            } else if self.fail == Failure::StageSync {
                Err(Self::error())
            } else {
                Filesystem.sync_stage(stage)
            }
        }
        fn link(&self, stage: &Path, destination: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("link");
            if self.fail == Failure::Link {
                Err(Self::error())
            } else {
                if self.fail == Failure::Race {
                    std::fs::write(destination, b"concurrent winner").unwrap();
                }
                Filesystem.link(stage, destination)
            }
        }
        fn remove_stage(&self, stage: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("unlink");
            if self.fail == Failure::Remove {
                Err(Self::error())
            } else {
                Filesystem.remove_stage(stage)
            }
        }
        fn sync_parent(&self, parent: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("parent_sync");
            if self.fail == Failure::ParentSync {
                Err(Self::error())
            } else {
                Filesystem.sync_parent(parent)
            }
        }
    }
    fn assert_archive(path: &Path, owner: u64) {
        let copied = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let epoch: Vec<u8> = copied
            .query_row("SELECT owner_epoch FROM metadata", [], |row| row.get(0))
            .unwrap();
        assert_eq!(epoch, owner.to_be_bytes());
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn prepublication_sync_and_link_faults_remove_stage_without_publishing() {
        for failure in [Failure::StageSync, Failure::Link] {
            let directory = Directory::new();
            let mut store = directory.store();
            let io = FaultIo::new(failure);
            assert_eq!(
                export(&mut store, &directory.archive(), &io),
                Err(BackupError::Unavailable)
            );
            assert!(!directory.archive().exists());
            assert!(directory.stages().is_empty());
            let expected = if failure == Failure::StageSync {
                vec!["stage_sync", "unlink"]
            } else {
                vec!["stage_sync", "link", "unlink"]
            };
            assert_eq!(*io.events.borrow(), expected);
            store.readiness().unwrap();
        }
    }

    #[test]
    fn publication_race_preserves_the_concurrent_destination() {
        let directory = Directory::new();
        let mut store = directory.store();
        let io = FaultIo::new(Failure::Race);
        assert_eq!(
            export(&mut store, &directory.archive(), &io),
            Err(BackupError::DestinationExists)
        );
        assert_eq!(
            std::fs::read(directory.archive()).unwrap(),
            b"concurrent winner"
        );
        assert!(directory.stages().is_empty());
        assert_eq!(*io.events.borrow(), ["stage_sync", "link", "unlink"]);
    }

    #[test]
    fn postpublication_cleanup_and_sync_faults_keep_a_private_valid_archive() {
        for failure in [Failure::Remove, Failure::ParentSync] {
            let directory = Directory::new();
            let mut store = directory.store();
            let owner = store.owner_generation();
            let io = FaultIo::new(failure);
            assert_eq!(
                export(&mut store, &directory.archive(), &io),
                Err(BackupError::PublicationUncertain)
            );
            assert_archive(&directory.archive(), owner);
            assert_eq!(
                directory.stages().len(),
                usize::from(failure == Failure::Remove)
            );
            let expected = if failure == Failure::Remove {
                vec!["stage_sync", "link", "unlink"]
            } else {
                vec!["stage_sync", "link", "unlink", "parent_sync"]
            };
            assert_eq!(*io.events.borrow(), expected);
            assert_eq!(
                store.export_backup(directory.archive()),
                Err(BackupError::DestinationExists)
            );
            store.readiness().unwrap();
        }
    }

    #[test]
    fn actual_sqlite_failure_keeps_only_private_stage_when_cleanup_also_fails() {
        let directory = Directory::new();
        let mut store = directory.store();
        store
            .connection
            .get_mut()
            .unwrap()
            .execute_batch("BEGIN")
            .unwrap();
        let io = FaultIo::new(Failure::Remove);
        assert_eq!(
            export(&mut store, &directory.archive(), &io),
            Err(BackupError::Unavailable)
        );
        assert!(!directory.archive().exists());
        let stages = directory.stages();
        assert_eq!(stages.len(), 1);
        assert_eq!(
            std::fs::metadata(&stages[0]).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(*io.events.borrow(), ["unlink"]);
        store
            .connection
            .get_mut()
            .unwrap()
            .execute_batch("ROLLBACK")
            .unwrap();
        store.readiness().unwrap();
    }

    #[test]
    fn failed_sync_removes_only_owned_stage_and_its_sqlite_sidecars() {
        let directory = Directory::new();
        let mut store = directory.store();
        let unrelated = directory.0.join("unrelated.sqlite3-wal");
        std::fs::write(&unrelated, b"caller file").unwrap();
        let mut io = FaultIo::new(Failure::SidecarsStageSync);
        io.parent = Some(directory.0.clone());
        assert_eq!(
            export(&mut store, &directory.archive(), &io),
            Err(BackupError::Unavailable)
        );
        assert!(!directory.archive().exists());
        assert!(directory.stages().is_empty());
        assert_eq!(std::fs::read(unrelated).unwrap(), b"caller file");
        assert_eq!(*io.events.borrow(), ["stage_sync", "unlink"]);
    }
}
