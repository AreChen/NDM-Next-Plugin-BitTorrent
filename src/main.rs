mod metadata;
mod options;
mod sharing;

use std::{collections::HashSet, io, path::PathBuf, sync::Arc, time::Duration};

use librqbit::{AddTorrent, AddTorrentOptions, Session, TorrentStats};
use ndm_plugin_sdk::{
    DownloadRequest, ENGINE_ARGUMENT, Engine, FailureReason, PeerStats, SourceFiles, TaskUpdate,
    TransferProgress, Worker, serve_engine,
};

struct BitTorrent;

fn main() -> io::Result<()> {
    if std::env::args().nth(1).as_deref() != Some(ENGINE_ARGUMENT) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "run this engine through OnesDM",
        ));
    }
    serve_engine(BitTorrent)
}

impl Engine for BitTorrent {
    fn download(
        &self,
        request: DownloadRequest,
        worker: &Worker,
    ) -> Result<Vec<String>, FailureReason> {
        run(&request, worker, false, async |session| {
            transfer(session, &request, worker).await
        })
    }

    fn inspect(
        &self,
        request: DownloadRequest,
        worker: &Worker,
    ) -> Result<SourceFiles, FailureReason> {
        run(&request, worker, true, async |session| {
            metadata::visible_files(&metadata::list(session, &request, false).await?)
        })
    }

    fn share(
        &self,
        request: DownloadRequest,
        output: PathBuf,
        worker: &Worker,
    ) -> Result<(), FailureReason> {
        run(&request, worker, false, async |session| {
            sharing::seed(session, &request, &output, worker).await
        })
    }
}

fn run<T>(
    request: &DownloadRequest,
    worker: &Worker,
    inspect: bool,
    operation: impl AsyncFnOnce(&Arc<Session>) -> Result<T, FailureReason>,
) -> Result<T, FailureReason> {
    let options = options::session(request, inspect)?;
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|_| FailureReason::Other)?
        .block_on(async {
            let session = Session::new_with_opts(request.workspace.clone(), options)
                .await
                .map_err(|_| FailureReason::Network)?;
            let result = tokio::select! {
                result = operation(&session) => result,
                () = stopped(worker) => Err(FailureReason::Other),
            };
            session.stop().await;
            result
        })
}

async fn stopped(worker: &Worker) {
    while !worker.stopped() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn transfer(
    session: &Arc<Session>,
    request: &DownloadRequest,
    worker: &Worker,
) -> Result<Vec<String>, FailureReason> {
    let payload = request.workspace.join("payload");
    let listing = metadata::list(session, request, true).await?;
    metadata::remember(request, &listing.torrent_bytes).await?;
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(listing.torrent_bytes),
            Some(AddTorrentOptions {
                output_folder: Some(payload.to_str().ok_or(FailureReason::Storage)?.to_owned()),
                only_files: listing.only_files,
                initial_peers: Some(listing.seen_peers),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .map_err(|_| FailureReason::InvalidSource)?
        .into_handle()
        .ok_or(FailureReason::Other)?;
    loop {
        let stats = handle.stats();
        if stats.error.is_some() {
            return Err(FailureReason::Network);
        }
        if let Some(live) = &stats.live {
            worker
                .progress(TransferProgress {
                    downloaded: stats.progress_bytes,
                    total: Some(stats.total_bytes),
                    transferred: live.snapshot.fetched_bytes,
                })
                .map_err(|_| FailureReason::Other)?;
            report_peers(worker, &stats)?;
        }
        if stats.finished {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let directory = handle
        .output_folder()
        .strip_prefix(&request.workspace)
        .map_err(|_| FailureReason::Storage)?;
    let selected: Option<HashSet<usize>> =
        handle.only_files().map(|files| files.into_iter().collect());
    let files = handle
        .with_metadata(|metadata| {
            metadata
                .file_infos
                .iter()
                .enumerate()
                .filter_map(|(index, info)| {
                    if selected
                        .as_ref()
                        .is_some_and(|files| !files.contains(&index))
                        || info.attrs.padding
                        || info.attrs.symlink
                    {
                        return None;
                    }
                    let path = directory.join(&info.relative_filename);
                    request
                        .workspace
                        .join(&path)
                        .is_file()
                        .then_some(metadata::relative(&path))
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|_| FailureReason::Storage)??;
    if files.is_empty() {
        Err(FailureReason::Storage)
    } else {
        Ok(files)
    }
}

pub(crate) fn report_peers(worker: &Worker, stats: &TorrentStats) -> Result<(), FailureReason> {
    let Some(live) = &stats.live else {
        return Ok(());
    };
    let peers = &live.snapshot.peer_stats;
    worker
        .update_task(TaskUpdate::PeerStats(PeerStats {
            connected: peers.live,
            seeders: peers.live_seeders,
            connecting: peers.connecting,
            queued: peers.queued,
            known: peers.seen,
            uploaded: stats.uploaded_bytes,
            upload_bytes_per_second: live.upload_speed.as_bytes(),
        }))
        .map_err(|_| FailureReason::Other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn output_paths_use_portable_slashes() {
        assert_eq!(
            metadata::relative(&Path::new("torrent").join("video.mp4")).unwrap(),
            "torrent/video.mp4"
        );
        assert!(metadata::relative(Path::new("../outside")).is_err());
    }
}
