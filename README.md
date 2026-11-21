# NDM Next BitTorrent plugin

This engine downloads magnet links and local or remote `.torrent` files
using librqbit. In NDM Next, choose any subset of files from a torrent or
magnet link, or use a filename regex for automated downloads. Resuming checks
existing pieces in the task workspace.

Options include DHT, local peer discovery, µTP, UPnP, download and upload
limits, and up to 128 simultaneous peers per task. Leave the peer limit
blank to inherit the host's thread setting. An open inbound port and more
available peers may improve throughput.

Completed tasks can seed on demand. The worker verifies the published files
and uploads verified pieces with read-only access. Selected-file downloads may
be missing pieces that overlap files you skipped. The worker exits after the
transfer or when stopped. Peer counts and upload statistics appear in the
task list and in download details.

Install from the NDM Next plugin market or import the matching release ZIP.
Paste a magnet link or use **Choose file** in the new-download form.
The BitTorrent download tool appears in MCP when AI integration is enabled.

Build with Rust 1.100.0. Use `cargo +1.100.0 test --locked` for local
verification. GitHub Actions publishes the platform archives and SHA-256
sidecars for each version tag.

## 中文

在新建下载中粘贴磁力链接或选择 `.torrent` 文件，点击 **选择下载文件** 勾选要保存的
文件。自动化仍可使用文件正则筛选。插件可配置 DHT、局域网发现、µTP、UPnP、下载限速、
上传限速和节点连接上限。节点上限留空时沿用 NDM Next 的线程数，每个任务最多 128 个节点。

下载完成后可手动开始或停止做种。引擎只读取已保存文件，并上传校验通过的分片。跨越未选文件的
分片可能无法做种。任务列表和独立下载窗口会显示连接数、上传信息和分享率。
