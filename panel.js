function panel(context) {
  if (context.scheme === "magnet") {
    return {fields: [{key: "dht"}, {key: "file-pattern"}]};
  }
  return {fields: [{key: "file-pattern"}, {key: "dht"}]};
}
