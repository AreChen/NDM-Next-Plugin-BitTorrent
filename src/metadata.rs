use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use librqbit::{AddTorrent, AddTorrentOptions, AddTorrentResponse, ListOnlyResponse, Session};
use ndm_plugin_sdk::{
    DownloadRequest, DownloadSource, ENGINE_STATE_DIRECTORY, FailureReason, SourceFile, SourceFiles,
};
use serde_json::Value;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

const METADATA_LIMIT: usize = 8 * 1024 * 1024;

pub(crate) async fn list(
    session: &Arc<Session>,
    request: &DownloadRequest,
    apply_filter: bool,
) -> Result<ListOnlyResponse, FailureReason> {
    let source = source(request).await?;
    let only_files_regex = (apply_filter && request.selection.is_none())
        .then(|| {
            request
                .options
                .get("file-pattern")
                .and_then(Value::as_str)
                .filter(|pattern| !pattern.is_empty())
                .map(str::to_owned)
        })
        .flatten();
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        session.add_torrent(
            source,
            Some(AddTorrentOptions {
                list_only: true,
                only_files_regex,
                ..Default::default()
            }),
        ),
    )
    .await
    .map_err(|_| FailureReason::Network)?
    .map_err(|_| FailureReason::InvalidSource)?;
    let AddTorrentResponse::ListOnly(mut listing) = response else {
        return Err(FailureReason::Other);
    };
    if let Some(selection) = &request.selection {
        if !selection.valid() || selection.revision != revision(&listing) {
            return Err(FailureReason::InvalidOptions);
        }
        let mut selected = selection.file_ids.iter().copied().peekable();
        for (idx, file) in listing.info.iter_file_details().enumerate() {
            if selected.peek().is_some_and(|id| *id as usize == idx) {
                if file.attrs().padding || file.attrs().symlink {
                    return Err(FailureReason::InvalidOptions);
                }
                selected.next();
            }
        }
        if selected.peek().is_some() {
            return Err(FailureReason::InvalidOptions);
        }
        listing.only_files = Some(selection.file_ids.iter().map(|id| *id as usize).collect());
    }
    Ok(listing)
}

pub(crate) fn visible_files(listing: &ListOnlyResponse) -> Result<SourceFiles, FailureReason> {
    let files = listing
        .info
        .iter_file_details()
        .enumerate()
        .filter_map(|(index, file)| {
            let attrs = file.attrs();
            (!attrs.padding && !attrs.symlink).then(|| {
                let path = relative(&file.filename.to_pathbuf())?;
                Ok(SourceFile {
                    id: index.try_into().map_err(|_| FailureReason::InvalidSource)?,
                    path: path
                        .chars()
                        .map(|ch| if ch.is_control() { '�' } else { ch })
                        .collect(),
                    size: file.len,
                })
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let source = SourceFiles {
        revision: revision(listing),
        files,
    };
    if !source.valid() {
        return Err(FailureReason::InvalidSource);
    }
    Ok(source)
}

fn revision(listing: &ListOnlyResponse) -> String {
    listing.info_hash.as_string()
}

fn torrent_path(request: &DownloadRequest) -> PathBuf {
    request
        .workspace
        .join(ENGINE_STATE_DIRECTORY)
        .join("source.torrent")
}

pub(crate) async fn remember(
    request: &DownloadRequest,
    torrent: &[u8],
) -> Result<(), FailureReason> {
    if torrent.is_empty() || torrent.len() > METADATA_LIMIT {
        return Err(FailureReason::InvalidSource);
    }
    let path = torrent_path(request);
    if path.is_file() {
        return Ok(());
    }
    let directory = request.workspace.join(ENGINE_STATE_DIRECTORY);
    let mut dir = fs::DirBuilder::new();
    #[cfg(unix)]
    dir.mode(0o700);
    dir.recursive(true)
        .create(directory)
        .await
        .map_err(|_| FailureReason::Storage)?;
    let pending = path.with_extension("pending");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&pending)
        .await
        .map_err(|_| FailureReason::Storage)?;
    file.write_all(torrent)
        .await
        .map_err(|_| FailureReason::Storage)?;
    file.sync_all().await.map_err(|_| FailureReason::Storage)?;
    drop(file);
    fs::rename(pending, path)
        .await
        .map_err(|_| FailureReason::Storage)
}

async fn source(request: &DownloadRequest) -> Result<AddTorrent<'_>, FailureReason> {
    for path in [
        torrent_path(request),
        request.workspace.join("source.torrent"),
    ] {
        if let Ok(bytes) = bounded_file(&path).await {
            return Ok(AddTorrent::from_bytes(bytes));
        }
    }
    if let DownloadSource::Url { url, .. } = &request.source
        && url.starts_with("magnet:")
    {
        return Ok(AddTorrent::from_url(url.as_str()));
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

pub(crate) fn relative(path: &Path) -> Result<String, FailureReason> {
    path.components()
        .map(|component| match component {
            Component::Normal(name) => name.to_str().ok_or(FailureReason::Storage),
            _ => Err(FailureReason::Storage),
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("/"))
}
