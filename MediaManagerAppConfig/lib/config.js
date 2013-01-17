var config = {
  db: {
    database: "plm-media-manager",
    local: {
      execPath: './MediaManagerTouchServer.app/Contents/MacOS/MediaManagerTouchServ',
      port: "59840"
    },
    remote: {
      host: "72.52.106.218",
      port: "5984"
    }
  }
};

module.exports = config;

