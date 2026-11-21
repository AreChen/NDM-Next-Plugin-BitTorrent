use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{Context, bail};
use librqbit::{
    AddTorrent, AddTorrentOptions, ManagedTorrentShared, Session, TorrentMetadata,
    storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage},
};
use ndm_plugin_sdk::{DownloadRequest, FailureReason, Worker};

pub(crate) async fn seed(
    session: &Arc<Session>,
    request: &DownloadRequest,
    output: &Path,
    worker: &Worker,
) -> Result<(), FailureReason> {
    let listing = crate::metadata::list(session, request, true).await?;
    let selected = listing.only_files.map(|files| files.into_iter().collect());
    let factory = PublishedFiles {
        output: output.to_path_buf(),
        selected,
    };
    let torrent = session
        .add_torrent(
            AddTorrent::from_bytes(listing.torrent_bytes),
            Some(AddTorrentOptions {
                paused: true,
                // Scan existing pieces but have no queued pieces to download.
                only_files: Some(Vec::new()),
                initial_peers: Some(listing.seen_peers),
                storage_factory: Some(factory.boxed()),
                ..Default::default()
            }),
        )
        .await
        .map_err(|_| FailureReason::Storage)?
        .into_handle()
        .ok_or(FailureReason::Other)?;
    torrent
        .wait_until_initialized()
        .await
        .map_err(|_| FailureReason::Storage)?;
    if !torrent.stats().file_progress.iter().any(|bytes| *bytes > 0) {
        return Err(FailureReason::Storage);
    }
    session
        .unpause(&torrent)
        .await
        .map_err(|_| FailureReason::Network)?;
    loop {
        let stats = torrent.stats();
        if stats.error.is_some() {
            return Err(FailureReason::Network);
        }
        crate::report_peers(worker, &stats)?;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[derive(Clone)]
struct PublishedFiles {
    output: PathBuf,
    selected: Option<HashSet<usize>>,
}

impl StorageFactory for PublishedFiles {
    type Storage = ReadOnlyStorage;

    fn create(
        &self,
        _: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<Self::Storage> {
        let output = fs::symlink_metadata(&self.output)?;
        if linked(&output) || !(output.is_file() || output.is_dir()) {
            bail!("invalid completed output");
        }
        let selected = |index| {
            self.selected
                .as_ref()
                .is_none_or(|files| files.contains(&index))
        };
        let count = metadata
            .file_infos
            .iter()
            .enumerate()
            .filter(|(index, info)| selected(*index) && !info.attrs.padding && !info.attrs.symlink)
            .count();
        if output.is_file() && count != 1 {
            bail!("completed single file does not match this torrent");
        }
        let files = metadata
            .file_infos
            .iter()
            .enumerate()
            .map(|(index, info)| {
                if !selected(index) || info.attrs.padding || info.attrs.symlink {
                    return Ok(None);
                }
                let target = if output.is_file() {
                    self.output.clone()
                } else {
                    regular_child(&self.output, &info.relative_filename)?
                };
                match File::open(&target) {
                    Ok(file) if file.metadata()?.len() == info.len => Ok(Some(Mutex::new(file))),
                    Ok(_) => Ok(None),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(error) => Err(error.into()),
                }
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(ReadOnlyStorage(Mutex::new(Some(files.into()))))
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

fn regular_child(root: &Path, relative: &Path) -> anyhow::Result<PathBuf> {
    let mut path = root.to_path_buf();
    let mut parts = relative.components().peekable();
    while let Some(Component::Normal(name)) = parts.next() {
        path.push(name);
        match fs::symlink_metadata(&path) {
            Ok(info)
                if !linked(&info)
                    && if parts.peek().is_some() {
                        info.is_dir()
                    } else {
                        info.is_file()
                    } => {}
            Ok(_) => bail!("invalid path in completed torrent"),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if parts.peek().is_none() {
            return Ok(path);
        }
    }
    bail!("invalid path in completed torrent")
}

fn linked(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.is_symlink()
    }
}

type OpenFiles = Arc<[Option<Mutex<File>>]>;
struct ReadOnlyStorage(Mutex<Option<OpenFiles>>);

impl TorrentStorage for ReadOnlyStorage {
    fn init(&mut self, _: &ManagedTorrentShared, _: &TorrentMetadata) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        let files = self.0.lock().unwrap().clone().context("storage stopped")?;
        let mut file = files
            .get(file_id)
            .and_then(Option::as_ref)
            .context("file unavailable for seeding")?
            .lock()
            .unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(buf)?;
        Ok(())
    }

    fn pwrite_all(&self, _: usize, _: u64, _: &[u8]) -> anyhow::Result<()> {
        bail!("seeding storage is read-only")
    }

    fn remove_file(&self, _: usize, _: &Path) -> anyhow::Result<()> {
        bail!("seeding storage is read-only")
    }

    fn remove_directory_if_empty(&self, _: &Path) -> anyhow::Result<()> {
        bail!("seeding storage is read-only")
    }

    fn ensure_file_length(&self, _: usize, _: u64) -> anyhow::Result<()> {
        bail!("seeding storage is read-only")
    }

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(Self(Mutex::new(self.0.lock().unwrap().take()))))
    }
}
