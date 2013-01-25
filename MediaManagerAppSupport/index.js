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

var _ = require('underscore');
var log4js = require('log4js');
var config = require('MediaManagerAppConfig');
var storage = require('./lib/storage.js');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore')(config);
var MediaManagerRouter = require('./lib/MediaManagerRouter.js');

var EventEmitter = require('events').EventEmitter;

var init = function(appjs, routes) {

  var app = Object.create(EventEmitter.prototype,
                          {appjs: { value: appjs },
                           config: { value: config }});

  if (_.has(config, 'logging')) {
    log4js.configure(config.logging);
    app.logger = log4js.getLogger('plm.MediaManagerApp');
  }
  else {
    app.logger = log4js.getLogger();
  }

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
