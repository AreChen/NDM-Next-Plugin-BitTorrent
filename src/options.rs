use std::num::NonZeroU32;

use librqbit::{
    DhtSessionConfig, ListenerMode, ListenerOptions, SessionOptions, limits::LimitsConfig,
};
use ndm_plugin_sdk::{DownloadRequest, FailureReason};
use serde_json::Value;

pub(crate) fn session(
    request: &DownloadRequest,
    inspect: bool,
) -> Result<SessionOptions, FailureReason> {
    let enabled = |key, fallback| {
        request
            .options
            .get(key)
            .and_then(Value::as_bool)
            .unwrap_or(fallback)
    };
    let limit = |key, maximum| {
        request
            .options
            .get(key)
            .map(|value| {
                value
                    .as_u64()
                    .filter(|value| *value <= maximum)
                    .map(|value| value as u32)
                    .ok_or(FailureReason::InvalidOptions)
            })
            .transpose()
    };
    let download = limit("download-limit", 1048576)?.unwrap_or_default();
    let upload = limit("upload-limit", 1048576)?.unwrap_or_default();
    let peers = limit("peer-limit", 128)?;
    if peers == Some(0) {
        return Err(FailureReason::InvalidOptions);
    }
    let listen = (!inspect || enabled("utp", false)).then(|| ListenerOptions {
        mode: if enabled("utp", false) {
            ListenerMode::TcpAndUtp
        } else {
            ListenerMode::TcpOnly
        },
        enable_upnp_port_forwarding: !inspect && enabled("upnp", false),
        ..Default::default()
    });
    Ok(SessionOptions {
        dht: enabled("dht", true).then(|| DhtSessionConfig {
            persistence: None,
            ..Default::default()
        }),
        listen,
        disable_local_service_discovery: !enabled("local-discovery", false),
        peer_limit: Some(
            peers.unwrap_or_else(|| request.download_threads.map_or(16, |n| n.get().min(128)))
                as usize,
        ),
        ratelimits: LimitsConfig {
            download_bps: NonZeroU32::new(download * 1024),
            upload_bps: NonZeroU32::new(upload * 1024),
        },
        ..Default::default()
    })
}
