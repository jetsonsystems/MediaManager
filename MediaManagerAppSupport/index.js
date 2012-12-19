//
//  MediaManagerAppSupport: App server side support code. IE:
//
//    - init: To be called on application start. Returns an app instance.
//
//    - app: An instance of our application.
//
//      methods:
//        config: Returns the application configuration.
//        shutdown: Do a graceful shutdown.
//
//      Emits the following events:
//        localStorageExit: The local storage process (DB) has exited, abnormally.
//        localStorageShutdown: The local storage process was shutdown normally.
//

var config = require('MediaManagerAppConfig');
var storage = require('./lib/storage.js');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore');
mmApi.config({dbHost: 'localhost',
              dbPort: config.db.local.port,
              dbName: config.db.database});
var MediaManagerRouter = require('./lib/MediaManagerRouter.js');

var EventEmitter = require('events').EventEmitter;

var init = function(appjs, routes) {
  var app = Object.create(EventEmitter.prototype);

  app.config = config;

  app.mediaManagerRouter = new MediaManagerRouter(appjs, routes);

  app.shutdown = function() {
    dataStore.shutdown();
  };

  var dataStore = storage.init(config);
  
  dataStore.on('localStorageExit', function() {
    app.emit('localStorageExit');
  });

  dataStore.on('localStorageShutdown', function() {
    app.emit('localStorageShutdown');
  });

  return app;
};

module.exports.init = init;
