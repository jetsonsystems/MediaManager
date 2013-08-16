//
//  MediaManagerAppSupport: App server side support code. 
//
//    Usage:
//
//      var app = require('MediaManagerAppSupport')(appjs, routes);
//
//    The app instance returned by invoking the module has the following interface:
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
//        appReady: App is completely initialized.
//

var path = require('path');
var fs = require('fs');

var _ = require('underscore');
var log4js = require('log4js');
var retry = require('retry');
//
// OSXFileSystem MUST be required before MediaManagerAppConfig!
//
var osxFs = require('./lib/OSXFileSystem');
var Worker = require('webworker');
var config = require('MediaManagerAppConfig');
var storage = require('./lib/storage.js');
var mmStorageModule = require('MediaManagerStorage');
var mmStorageInst = mmStorageModule(config.db, {singleton: true});
var mmStorage = mmStorageInst.get('touchdb');
var fileCacheAlias = config.storage["file-cache"].alias;
var mmStorageFileCache = mmStorageInst.get('file-cache',
                                           { 
                                             singleton: true,
                                             alias: fileCacheAlias
                                           });
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore')(config);
var MediaManagerRouter = require('./lib/MediaManagerRouter.js');
var AppServWorkerMessages = require('./lib/AppServWorkerMessages.js');

var EventEmitter = require('events').EventEmitter;

var log = log4js.getLogger('plm.MediaManagerAppSupport');

var MediaManagerAppSupportModule = function(appjs, routes) {

  var logPrefix = 'init: ';

  var app = Object.create(EventEmitter.prototype,
                          {
                            readyStates: { value: {
                              UNINITIALIZED: 0,
                              INITIALIZING: 1,
                              READY: 2
                            }},
                            readyState: { value: 0,
                                          writable: true },
                            appjs: { value: appjs },
                            config: { value: config },
                            servWorker: { value: undefined,
                                          writable: true},
                            servWorkerReady: { value: false,
                                               writable: true }
                          });

  app.logger = log4js.getLogger('plm.MediaManagerApp');

  log.info(logPrefix + 'initializing...');

  app.readyState = app.readyStates.INITIALIZING;

  app.mediaManagerRouter = new MediaManagerRouter(appjs, routes);

  //
  // Initialize the file cache.
  //
  app.fileCache = mmStorageFileCache;
  //
  // Ensure the file-cache alias has a link in ./assets/<alias> -> app.fileCache.rootDir
  //
  var aliasPath = path.join('./assets/', fileCacheAlias);

  if (!fs.existsSync('assets')) {
    new Error(logPrefix + 'Unable to locate assets directory!');
  }

  if (!fs.existsSync(aliasPath)) {
    try {
      fs.symlinkSync(app.fileCache.rootDir, aliasPath);
    }
    catch (e) {
      log.error(logPrefix + 'File cache alias creation error, alias path - ' + aliasPath + ', file cache root dir - ' + app.fileCache.rootDir);
      app.emit('fileCacheAliasCreateError');
    }
  }

  var aliasStat = fs.lstatSync(aliasPath);

  if (!aliasStat.isSymbolicLink()) {
    log.error(logPrefix + 'File cache alias not a valid sym link, alias path - ' + aliasPath + ', file cache root dir - ' + app.fileCache.rootDir);
    app.emit('fileCacheAliasTypeError');
  }

  app.storage = {
    //
    //  readyState constants:
    //
    UNINITIALIZED: 0,
    DATA_STORE_READY: 1,
    INIT_ERROR: 2,
    READY: 3,

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
    dataStore: null
  };

  app.storage.readyState = app.storage.UNINITIALIZED;

  var dataStore = null;

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
      //
      //  We need to get DB info initialized,. and once that happens,
      //  the changes feed can be 'created' with the DB sequence ID
      //  set to the since parameter.
      //
      onDbInfo = function(err, infoObj) {
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
            //  Get the appServWorker going.
            //
            var workerPath = path.join(__dirname, 'lib/AppServWorker.js');
            log.info('MediaManagerAppSupport: Starting AppServWorker with path - ' + workerPath);
            app.servWorker = new Worker(workerPath);
            app.servWorker.onmessage = function(e) {
              if (e.data.type === AppServWorkerMessages.APP_SERVER_READY) {
                app.servWorkerReady = true;
                app.readyState = app.readyStates.READY;
                app.emit('appReady');
              }
            };
            app.servWorker.postMessage({
              "type": AppServWorkerMessages.LOCAL_STORAGE_READY,
              "appId": config.app.id,
              "dbInfo": {
                "dbName": currentInfo.dbName,
                "docCount": currentInfo.docCount,
                "updateSeq": currentInfo.updateSeq,
                "diskSize": currentInfo.diskSkize
              }
            });
            app.servWorker.onexit = function(c, s) {
              log.error('MediaManagerAppSupport: AppServWorker exited, code - ' + c + ', signal - ' + s);
              app.emit('appServWorkerExit');
            };
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
      };

      var op = retry.operation({
        retries: 10
      });

      var getDbInfo = function(currentAttempt) {
        mmStorage.info(function(err, infoObj) {
          if (op.retry(err)) {
            return;
          }

          onDbInfo(err ? op.mainError() : null, infoObj);
        });
      };

      op.attempt(getDbInfo);
    }
    catch (err) {
      app.storage.readyState = app.storage.INIT_ERROR;
      log.error('MediaManagerAppSupport: Storage init error - ' + err);
      app.emit('localStorageInitError');
    }
  }
      
  app.shutdown = function() {
    if (this.servWorker) {
      this.servWorker.terminate();
    }
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

module.exports = MediaManagerAppSupportModule;
