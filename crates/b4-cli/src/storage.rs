use crate::config::LauncherError;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::{
    fs::{self, OpenOptions},
    io,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
};

#[cfg(unix)]
pub(crate) fn validate(path: &Path) -> Result<(), LauncherError> {
    validate_at(&resolve(path)?, false).map(|_| ())
}

#[cfg(unix)]
pub(crate) fn prepare(path: &Path) -> Result<PathBuf, LauncherError> {
    let path = resolve(path)?;
    if !validate_at(&path, false)? {
        match fs::DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => {
                // The canonical parent is trusted and stable. Only this new leaf
                // is ours to repair; restrictive umask may have created it as 0000.
                fs::set_permissions(&path, fs::Permissions::from_mode(0o700))
                    .map_err(|_| LauncherError::InvalidStorage)?;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(LauncherError::InvalidStorage),
        }
    }
    validate_at(&path, false)?;
    for name in ["runtime.sqlite3", "owner.lock"] {
        reserve_file(&path.join(name))?;
    }
    validate_at(&path, true)?;
    Ok(path)
}

#[cfg(unix)]
pub(crate) fn validate_opened(path: &Path) -> Result<(), LauncherError> {
    validate_at(&resolve(path)?, true).map(|_| ())
}

#[cfg(not(unix))]
pub(crate) fn validate(_: &Path) -> Result<(), LauncherError> {
    Err(LauncherError::UnsupportedPlatform)
}
#[cfg(not(unix))]
pub(crate) fn prepare(_: &Path) -> Result<PathBuf, LauncherError> {
    Err(LauncherError::UnsupportedPlatform)
}
#[cfg(not(unix))]
pub(crate) fn validate_opened(_: &Path) -> Result<(), LauncherError> {
    Err(LauncherError::UnsupportedPlatform)
}

#[cfg(unix)]
fn resolve(path: &Path) -> Result<PathBuf, LauncherError> {
    let invalid = LauncherError::InvalidStorage;
    let text = path.to_str().ok_or(invalid)?;
    let leaf = text.rsplit('/').next().ok_or(invalid)?;
    if leaf.is_empty() || leaf == "." || leaf == ".." || text.contains('\0') {
        return Err(invalid);
    }
    let parent = path.parent().ok_or(invalid)?;
    let parent = fs::canonicalize(if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    })
    .map_err(|_| invalid)?;
    if !fs::metadata(&parent).map_err(|_| invalid)?.is_dir() {
        return Err(invalid);
    }
    Ok(parent.join(leaf))
}

#[cfg(unix)]
fn validate_at(path: &Path, opened: bool) -> Result<bool, LauncherError> {
    let invalid = LauncherError::InvalidStorage;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !opened => return Ok(false),
        Err(_) => return Err(invalid),
    };
    if !metadata.file_type().is_dir() || metadata.permissions().mode() & 0o7777 != 0o700 {
        return Err(invalid);
    }
    for name in ["runtime.sqlite3", "owner.lock"] {
        private_file(&path.join(name), opened)?;
    }
    for name in [
        "runtime.sqlite3-journal",
        "runtime.sqlite3-wal",
        "runtime.sqlite3-shm",
    ] {
        private_file(&path.join(name), false)?;
    }
    Ok(true)
}

#[cfg(unix)]
fn private_file(path: &Path, required: bool) -> Result<(), LauncherError> {
    let invalid = LauncherError::InvalidStorage;
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_file()
                && metadata.permissions().mode() & 0o7777 == 0o600 =>
        {
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound && !required => Ok(()),
        _ => Err(invalid),
    }
}

#[cfg(unix)]
fn reserve_file(path: &Path) -> Result<(), LauncherError> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(file) => file
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| LauncherError::InvalidStorage),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => private_file(path, true),
        Err(_) => Err(LauncherError::InvalidStorage),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use b4_runtime::Store;
    use b4_sqlite::SqliteStore;
    use std::{
        fs,
        os::unix::fs::{symlink, DirBuilderExt, PermissionsExt},
        process::Command,
        sync::atomic::{AtomicU64, Ordering},
    };

    struct TestDirectory(PathBuf);
    impl TestDirectory {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "b4-launcher-storage-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
            Self(path)
        }
        fn data(&self) -> PathBuf {
            self.0.join("data")
        }
    }
    impl Drop for TestDirectory {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn mode(path: &Path) -> u32 {
        fs::symlink_metadata(path).unwrap().permissions().mode() & 0o7777
    }
    fn private_dir(path: &Path) {
        fs::create_dir(path).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    fn private_file(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn readonly_validation_of_missing_leaf_never_creates_files() {
        let root = TestDirectory::new();
        let path = root.data();
        assert_eq!(validate(&path), Ok(()));
        assert!(!path.exists());
        assert_eq!(fs::read_dir(&root.0).unwrap().count(), 0);
    }

    #[test]
    fn post_open_validation_requires_reserved_database_and_owner_files() {
        let root = TestDirectory::new();
        let data = root.data();
        assert_eq!(validate_opened(&data), Err(LauncherError::InvalidStorage));
        private_dir(&data);
        assert_eq!(validate_opened(&data), Err(LauncherError::InvalidStorage));
        private_file(&data.join("runtime.sqlite3"), b"opaque bytes");
        assert_eq!(validate_opened(&data), Err(LauncherError::InvalidStorage));
        assert_eq!(fs::read_dir(&data).unwrap().count(), 1);
    }

    #[test]
    fn readonly_validation_never_opens_or_changes_existing_database_bytes() {
        let root = TestDirectory::new();
        let data = root.data();
        private_dir(&data);
        for name in [
            "runtime.sqlite3",
            "owner.lock",
            "runtime.sqlite3-journal",
            "runtime.sqlite3-wal",
            "runtime.sqlite3-shm",
        ] {
            let path = data.join(name);
            private_file(&path, b"opaque prior bytes, deliberately not SQLite");
        }
        assert_eq!(validate(&data), Ok(()));
        assert_eq!(validate_opened(&data), Ok(()));
        for entry in fs::read_dir(&data).unwrap() {
            let path = entry.unwrap().path();
            assert_eq!(
                fs::read(&path).unwrap(),
                b"opaque prior bytes, deliberately not SQLite"
            );
            assert_eq!(mode(&path), 0o600);
        }
    }

    #[test]
    fn final_leaf_and_existing_parent_are_required_without_mutation() {
        let root = TestDirectory::new();
        for path in [
            PathBuf::new(),
            PathBuf::from("/"),
            PathBuf::from("."),
            PathBuf::from(".."),
            root.0.join("."),
            root.0.join(".."),
            PathBuf::from(format!("{}/", root.0.display())),
            root.0.join("absent").join("data"),
        ] {
            assert_eq!(validate(&path), Err(LauncherError::InvalidStorage));
            assert_eq!(prepare(&path), Err(LauncherError::InvalidStorage));
        }
        assert_eq!(fs::read_dir(&root.0).unwrap().count(), 0);
    }

    #[test]
    fn prepare_reserves_private_leaf_and_files_preserving_siblings() {
        let root = TestDirectory::new();
        let sibling = root.0.join("unrelated");
        fs::write(&sibling, b"unchanged sibling").unwrap();
        let sibling_mode = mode(&sibling);
        let data = root.data();
        let prepared = prepare(&data).expect("private reservation");
        assert_eq!(prepared, fs::canonicalize(&data).unwrap());
        assert_eq!(mode(&data), 0o700);
        for name in ["runtime.sqlite3", "owner.lock"] {
            assert_eq!(mode(&data.join(name)), 0o600);
            assert_eq!(fs::read(data.join(name)).unwrap(), b"");
        }
        assert_eq!(fs::read(&sibling).unwrap(), b"unchanged sibling");
        assert_eq!(mode(&sibling), sibling_mode);
    }

    #[test]
    fn existing_broad_or_symlink_directory_is_refused_unchanged() {
        let root = TestDirectory::new();
        let data = root.data();
        private_dir(&data);
        fs::set_permissions(&data, fs::Permissions::from_mode(0o750)).unwrap();
        assert_eq!(validate(&data), Err(LauncherError::InvalidStorage));
        assert_eq!(prepare(&data), Err(LauncherError::InvalidStorage));
        assert_eq!(mode(&data), 0o750);
        assert_eq!(fs::read_dir(&data).unwrap().count(), 0);
        let link = root.0.join("link");
        symlink(&data, &link).unwrap();
        assert_eq!(validate(&link), Err(LauncherError::InvalidStorage));
        assert_eq!(prepare(&link), Err(LauncherError::InvalidStorage));
        assert!(fs::symlink_metadata(&link).unwrap().is_symlink());
    }

    #[test]
    fn existing_files_and_sidecars_must_be_regular_nonsymlink_exact_private() {
        for name in [
            "runtime.sqlite3",
            "owner.lock",
            "runtime.sqlite3-journal",
            "runtime.sqlite3-wal",
            "runtime.sqlite3-shm",
        ] {
            for kind in ["broad", "narrow", "directory", "symlink"] {
                let root = TestDirectory::new();
                let data = root.data();
                private_dir(&data);
                let path = data.join(name);
                match kind {
                    "broad" | "narrow" => {
                        private_file(&path, b"preserved bytes");
                        fs::set_permissions(
                            &path,
                            fs::Permissions::from_mode(if kind == "broad" { 0o640 } else { 0o400 }),
                        )
                        .unwrap();
                    }
                    "directory" => private_dir(&path),
                    "symlink" => {
                        let target = root.0.join("unrelated");
                        private_file(&target, b"preserved target");
                        symlink(&target, &path).unwrap();
                    }
                    _ => unreachable!(),
                }
                let previous_mode = mode(&path);
                assert_eq!(validate(&data), Err(LauncherError::InvalidStorage));
                assert_eq!(prepare(&data), Err(LauncherError::InvalidStorage));
                assert_eq!(validate_opened(&data), Err(LauncherError::InvalidStorage));
                assert_eq!(mode(&path), previous_mode);
                assert_eq!(fs::read_dir(&data).unwrap().count(), 1);
                if kind == "broad" || kind == "narrow" {
                    assert_eq!(fs::read(&path).unwrap(), b"preserved bytes");
                }
                if kind == "symlink" {
                    assert!(fs::symlink_metadata(&path).unwrap().is_symlink());
                    assert_eq!(
                        fs::read(root.0.join("unrelated")).unwrap(),
                        b"preserved target"
                    );
                }
            }
        }
    }

    #[test]
    fn prepare_reuses_exact_private_files_without_truncating_or_chmod() {
        let root = TestDirectory::new();
        let data = root.data();
        private_dir(&data);
        for name in ["runtime.sqlite3", "owner.lock"] {
            private_file(&data.join(name), b"preserved bytes");
        }
        assert!(prepare(&data).is_ok());
        for name in ["runtime.sqlite3", "owner.lock"] {
            assert_eq!(fs::read(data.join(name)).unwrap(), b"preserved bytes");
            assert_eq!(mode(&data.join(name)), 0o600);
        }
    }

    #[test]
    fn restrictive_umask_is_repaired_only_on_owned_fresh_entries() {
        let root = TestDirectory::new();
        let status = Command::new("/bin/sh")
            .args(["-c", "umask 0777; exec \"$@\"", "b4-storage-umask"])
            .arg(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::tests::subprocess_storage_probe",
                "--nocapture",
            ])
            .env("B4_PRIVATE_STORAGE_TEST_ROLE", "restrictive")
            .env("B4_PRIVATE_STORAGE_TEST_PATH", root.data())
            .status()
            .unwrap();
        assert!(status.success(), "fresh restrictive creation failed");
        assert_eq!(mode(&root.data()), 0o700);
        assert_eq!(mode(&root.data().join("runtime.sqlite3")), 0o600);
        assert_eq!(mode(&root.data().join("owner.lock")), 0o600);
    }

    #[test]
    fn actual_second_process_exclusion_preserves_owner_generation_and_bytes() {
        let root = TestDirectory::new();
        let data = prepare(&root.data()).unwrap();
        let store = SqliteStore::open(&data).unwrap();
        let generation = store.owner_generation();
        let prior = fs::read(data.join("runtime.sqlite3")).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "storage::tests::subprocess_storage_probe",
                "--nocapture",
            ])
            .env("B4_PRIVATE_STORAGE_TEST_ROLE", "contender")
            .env("B4_PRIVATE_STORAGE_TEST_PATH", &data)
            .status()
            .unwrap();
        assert!(status.success(), "contender exclusion failed");
        assert_eq!(store.owner_generation(), generation);
        assert_eq!(fs::read(data.join("runtime.sqlite3")).unwrap(), prior);
        assert_eq!(validate(&data), Ok(()));
        assert_eq!(validate_opened(&data), Ok(()));
        drop(store);
        let reopened = SqliteStore::open(&data).unwrap();
        assert!(reopened.owner_generation() > generation);
    }

    #[test]
    fn subprocess_storage_probe() {
        let Some(role) = std::env::var_os("B4_PRIVATE_STORAGE_TEST_ROLE") else {
            return;
        };
        let path = PathBuf::from(
            std::env::var_os("B4_PRIVATE_STORAGE_TEST_PATH").expect("private fixture path"),
        );
        match role.to_str().expect("private fixture role") {
            "restrictive" => {
                assert!(prepare(&path).is_ok(), "private preparation failed");
            }
            "contender" => {
                assert!(prepare(&path).is_ok(), "private reuse failed");
                assert!(
                    matches!(
                        SqliteStore::open(&path),
                        Err(b4_runtime::StoreError::AlreadyOwned)
                    ),
                    "owner exclusion failed"
                );
            }
            _ => panic!("unknown private fixture role"),
        }
    }
}
