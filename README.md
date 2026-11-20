# NDM Next BitTorrent plugin

This engine downloads magnet links and local or remote `.torrent` files
using librqbit. It supports optional DHT discovery and file filtering.
Resuming checks existing pieces in the task workspace. The NDM Next thread
setting limits simultaneous peer connections, up to the plugin's limit.
The engine disconnects from the swarm when the task finishes or stops.

Install from the NDM Next plugin market or import the matching release ZIP.
Paste a magnet link or use **Choose file** in the new-download form.
The BitTorrent download tool appears in MCP when AI integration is enabled.

Build with Rust 1.100.0. Use `cargo +1.100.0 test --locked` for local
verification. GitHub Actions publishes the platform archives and SHA-256
sidecars for each version tag.
