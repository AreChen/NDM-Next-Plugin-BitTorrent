use std::{
    io,
    path::{Component, Path},
    sync::Arc,
    time::Duration,
};

use librqbit::{
    AddTorrent, AddTorrentOptions, DhtSessionConfig, ListenerOptions, Session, SessionOptions,
};
use ndm_plugin_sdk::{
    DownloadRequest, DownloadSource, ENGINE_ARGUMENT, FailureReason, TransferProgress, Worker,
    serve,
};
use serde_json::Value;
use tokio::{fs, io::AsyncReadExt};

const METADATA_LIMIT: usize = 8 * 1024 * 1024;

fn main() -> io::Result<()> {
    if std::env::args().nth(1).as_deref() != Some(ENGINE_ARGUMENT) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "run this engine through NDM Next",
        ));
    }
    serve(|request, worker| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|_| FailureReason::Other)?
            .block_on(download(request, worker))
    })
}

async fn download(request: DownloadRequest, worker: &Worker) -> Result<Vec<String>, FailureReason> {
    let discover = request
        .options
        .get("dht")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let dht = discover.then(|| DhtSessionConfig {
        persistence: None,
        ..Default::default()
    });
    let session = Session::new_with_opts(
        request.workspace.clone(),
        SessionOptions {
            dht,
            listen: Some(ListenerOptions::default()),
            disable_local_service_discovery: true,
            peer_limit: Some(request.download_threads.map_or(16, |n| n.get().min(128)) as usize),
            ..Default::default()
        },
    )
    .await
    .map_err(|_| FailureReason::Network)?;
    let result = tokio::select! {
        result = transfer(&session, &request, worker) => result,
        () = stopped(worker) => Err(FailureReason::Other),
    };
    session.stop().await;
    result
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
    let source = source(request).await?;
    let file_filter = request
        .options
        .get("file-pattern")
        .and_then(Value::as_str)
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_owned);
    let handle = session
        .add_torrent(
            source,
            Some(AddTorrentOptions {
                output_folder: Some(payload.to_str().ok_or(FailureReason::Storage)?.to_owned()),
                only_files_regex: file_filter,
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
        let transferred = if let Some(live) = stats.live.as_ref() {
            live.snapshot.fetched_bytes
        } else {
            0
        };
        let _ = worker.progress(TransferProgress {
            downloaded: stats.progress_bytes,
            total: (stats.total_bytes > 0).then_some(stats.total_bytes),
            transferred,
        });
        if stats.finished {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let directory = handle
        .output_folder()
        .strip_prefix(&request.workspace)
        .map_err(|_| FailureReason::Storage)?;
    let selected = handle.only_files();
    let files = handle
        .with_metadata(|metadata| {
            metadata
                .file_infos
                .iter()
                .enumerate()
                .filter_map(|(index, info)| {
                    if selected
                        .as_ref()
                        .is_some_and(|selected| !selected.contains(&index))
                    {
                        return None;
                    }
                    let path = directory.join(&info.relative_filename);
                    request
                        .workspace
                        .join(&path)
                        .is_file()
                        .then_some(relative(&path))
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

async fn source(request: &DownloadRequest) -> Result<AddTorrent<'_>, FailureReason> {
    if let DownloadSource::Url { url, .. } = &request.source
        && url.starts_with("magnet:")
    {
        return Ok(AddTorrent::from_url(url.as_str()));
    }
    let path = request.workspace.join("source.torrent");
    if let Ok(bytes) = bounded_file(&path).await {
        return Ok(AddTorrent::from_bytes(bytes));
    }
    let data = match &request.source {
        DownloadSource::File { path } => bounded_file(path).await?,
        DownloadSource::Url { url, headers } => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .map_err(|_| FailureReason::Network)?;
            let mut download = client.get(url);
            for (key, value) in headers {
                download = download.header(key, value);
            }
            let mut response = download
                .send()
                .await
                .and_then(reqwest::Response::error_for_status)
                .map_err(|_| FailureReason::Network)?;
            let mut data = Vec::new();
            while let Some(bytes) = response.chunk().await.map_err(|_| FailureReason::Network)? {
                if data.len().saturating_add(bytes.len()) > METADATA_LIMIT {
                    return Err(FailureReason::InvalidSource);
                }
                data.extend_from_slice(&bytes);
            }
            data
        }
    };
    if data.is_empty() {
        return Err(FailureReason::InvalidSource);
    }
    let pending = path.with_extension("pending");
    fs::write(&pending, &data)
        .await
        .map_err(|_| FailureReason::Storage)?;
    fs::rename(pending, path)
        .await
        .map_err(|_| FailureReason::Storage)?;
    Ok(AddTorrent::from_bytes(data))
}

async fn bounded_file(file: &Path) -> Result<Vec<u8>, FailureReason> {
    let mut bytes = Vec::new();
    fs::File::open(file)
        .await
        .map_err(|_| FailureReason::InvalidSource)?
        .take(METADATA_LIMIT as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| FailureReason::Storage)?;
    if (1..=METADATA_LIMIT).contains(&bytes.len()) {
        Ok(bytes)
    } else {
        Err(FailureReason::InvalidSource)
    }
}

fn relative(path: &Path) -> Result<String, FailureReason> {
    path.components()
        .map(|component| match component {
            Component::Normal(name) => name.to_str().ok_or(FailureReason::Storage),
            _ => Err(FailureReason::Storage),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn output_paths_use_portable_slashes() {
        assert_eq!(
            relative(&Path::new("torrent").join("video.mp4")).unwrap(),
            "torrent/video.mp4"
        );
        assert!(relative(Path::new("../outside")).is_err());
    }
}
