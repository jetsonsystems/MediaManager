var config = {
  db: {
    database: "plm-media-manager",
    local: {
      execPath: './MediaManagerTouchServer.app/Contents/MacOS/MediaManagerTouchServ',
      port: "59840"
    },
    remote: {
      host: "72.52.106.218"
    }
  },
  logging: {
    "appenders": [
      {
        "type": "console",
        "category": "console"
      },
      {
        "type": "file",
        "filename": "var/log/plm-media-manager.log",
        "backups": 10,
        "category": ["plm.MediaManagerApp", "plm.MediaManagerAppSupport", "plm.ImageService"]
      }
    ],
    "levels": { "plm.MediaManagerApp" : "TRACE",
                "plm.ImageService" : "ERROR" },
    "replaceConsole": false
  }
};

module.exports = config;

