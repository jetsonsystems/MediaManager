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
//        localStorageInitError: Error occurred during initialization.
//        localStorageReady: Local storage is ready to use.
//

var _ = require('underscore');
var log4js = require('log4js');
var config = require('MediaManagerAppConfig');
var storage = require('./lib/storage.js');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore')(config);
var mmStorageModule = require('MediaManagerStorage');
var mmStorage = mmStorageModule(config.db, {singleton: true});
var MediaManagerRouter = require('./lib/MediaManagerRouter.js');

var EventEmitter = require('events').EventEmitter;

var log = log4js.getLogger('plm.MediaManagerAppSupport');

var init = function(appjs, routes) {

  var app = Object.create(EventEmitter.prototype,
                          {appjs: { value: appjs },
                           config: { value: config }});

	/*
  if (_.has(config, 'logging')) {
    log4js.configure(config.logging);
    app.logger = log4js.getLogger('plm.MediaManagerApp');
  }
  else {
    app.logger = log4js.getLogger();
  }
	*/

  app.logger = log4js.getLogger('plm.MediaManagerApp');

  log.info('MediaManagerAppSupport: initializing...');

  app.mediaManagerRouter = new MediaManagerRouter(appjs, routes);

  app.storage = {
    //
    //  readyState constants:
    //
    UNINITIALIZED: 0,
    DATA_STORE_READY: 1,
    CHANGES_FEED_CREATED: 2,
    INIT_ERROR: 3,
    READY: 4,

    readyState: undefined,
    //
    //  DB info: Some basic DB stats. Note, currently, only the initial field is updated.
    //    In the future, we may periodically update current at various points in time.
    //
    //    Each contains these attributes:
    //      dbName
    //      docCount
    //      updateSeq
    //      diskSize
    //
    info: {
      initial: null,
      current: null
    },
    dataStore: null,
    changesFeed: null
  };

  app.storage.readyState = app.storage.UNINITIALIZED;

  var dataStore = null;
  var changesFeed = null;

  try {
    dataStore = storage.init(config);
    app.storage.dataStore = dataStore;
    app.storage.readyState = app.storage.DATA_STORE_READY;
    log.info('MediaManagerAppSupport: data store created...');
  }
  catch (err) {
    app.storage.readyState = app.storage.INIT_ERROR;
    app.emit('localStorageInitError');
  }

  if (app.storage.readyState != app.storage.INIT_ERROR) {
    try {
      changesFeed = new mmApi.StorageChangesFeed('/storage/changes-feed',
                                                 {instName: 'changes-feed',
                                                  pathPrefix: '/v0' })
      app.storage.changesFeed = changesFeed;
      app.storage.readyState = app.storage.CHANGES_FEED_CREATED;

      log.info('MediaManagerAppSupport: Changes feed created...');

      //
      //  We need to get DB info initialized,. and once that happens,
      //  the changes feed can be 'created' with the DB sequence ID
      //  set to the since parameter.
      //
      mmStorage.info(function(err, infoObj) {
        if (err) {
          app.storage.readyState = app.storage.INIT_ERROR
          log.error('MediaManagerAppSupport: Error retrieving storage info - ' + err);
          app.emit('localStorageInitError');
        }
        else {
          function createInfo() {
            var dbName = _.has(infoObj, 'dbName') ? infoObj.dbName : undefined;
            var docCount = _.has(infoObj, 'docCount') ? infoObj.docCount : undefined;
            var updateSeq = _.has(infoObj, 'updateSeq') ? infoObj.updateSeq : undefined;
            var diskSize = _.has(infoObj, 'diskSize') ? infoObj.diskSize : undefined;

            var iObj = Object.create({},
                                     { dbName: { value: dbName },
                                       docCount: { value: docCount },
                                       updateSeq: { value: updateSeq },
                                       diskSize: { value: diskSize } });
            Object.freeze(iObj);
            return iObj;

          };

          var currentInfo = createInfo();
          app.storage.info.initial = currentInfo;

          log.info('MediaManagerAppSupport: Initial database info., dbName - ' + currentInfo.dbName + ', docCount - ' + currentInfo.docCount + ', updateSeq - ' + currentInfo.updateSeq + ', diskSize - ' + currentInfo.diskSize);

          try {
            //
            //  Do a create request on the changes feed to start monitoring it.
            //
            log.info('MediaManagerAppSupport: Connecting to changes feed, w/ sequence ID - ' + app.storage.info.initial.updateSeq);

            app.storage.changesFeed.create({}, 
                                           { query: { since: currentInfo.updateSeq } });
            app.storage.readyState = app.storage.READY;
            log.info('MediaManagerAppSupport: Storage now ready to be used...');
            app.emit('localStorageReady');
          }
          catch (createErr) {
            app.storage.readyState = app.storage.INIT_ERROR;
            log.error('MediaManagerAppSupport: Storage initialization error whiling connecting to changes feed - ' + createErr);
            app.emit('localStorageInitError');
          }
        }
      });
    }
    catch (err) {
      app.storage.readyState = app.storage.INIT_ERROR;
      log.error('MediaManagerAppSupport: Storage init error - ' + err);
      app.emit('localStorageInitError');
    }
  }
      
  app.shutdown = function() {
    if (dataStore) {
      dataStore.shutdown();
    }
  };

      
  dataStore.on('localStorageExit', function() {
    app.emit('localStorageExit');
  });

  dataStore.on('localStorageShutdown', function() {
    app.emit('localStorageShutdown');
  });

  return app;
};

module.exports.init = init;
