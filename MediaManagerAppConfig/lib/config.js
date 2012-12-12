var config = {
  db: {
    database: "plm-media-manager",
    local: {
      execPath: './MediaManagerTouchServer.app/Contents/MacOS/MediaManagerTouchServ',
      port: "59840"
    },
    remote: {
      host: "localhost",
      port: "5984"
    }
  }
};

module.exports = config;

