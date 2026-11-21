# BitTorrent

Use `ndm_bittorrent__download` for a magnet URI, a remote `.torrent` URL, or
the absolute path to a local `.torrent` file. For a mixed batch, set
`"plugin":"bittorrent"` on the applicable `ndm_download` items. Leave
`destination` out to use NDM Next's configured folder.

- To download matching files only, use `options.file-pattern` with a filename
  regular expression. For example, pass `"options":{"file-pattern":"[.]mkv$"}`
  for MKV files. To pick exact files interactively, use **Choose files** in the
  desktop app.
- `download_threads` sets the maximum simultaneous peer count for this task
  unless `options.peer-limit` is set. The engine caps the count at 128.
- `options.download-limit` and `options.upload-limit` are in KiB/s. Use zero
  for unlimited. DHT, local peer discovery, µTP and UPnP are optional. Omit
  values so the current app and plugin defaults apply.
- Use the task ID to check progress. The app can start and stop seeding
  completed tasks. The plugin download tool queues downloads.
