'use strict';

var async = require('async')
  ,should = require('should')
  ,localDatabaseManager = require("./touchdbManager")
  ,remoteDatabaseManager = require("./databaseManager");

var config = {
  db: {
    database: "plm-media-manager-dev0",
    local: {
      execPath: "/Users/marekjulian/Applications/MediaManagerTouchServer.app/Contents/MacOS/MediaManagerTouchServ",
      port: "59840"
    },
    remote: {
      host: "localhost",
      port: "5984"
    }
  }
};

var mmApi = require('../lib/MediaManagerApiCore')(config, { singleton: false });

var syncResource = null;

var WebSocket = require('../lib/NotificationsWsLike');

function isConnectionEstablished(parsedMsg) {
  return (parsedMsg.resource === '/notifications' && parsedMsg.event === 'connection.established');
};

function isSyncStarted(parsedMsg) {
  return (parsedMsg.resource === '/storage/synchronizers' && parsedMsg.event === 'sync.started');
};

function isSyncCompleted(parsedMsg) {
  return (parsedMsg.resource === '/storage/synchronizers' && parsedMsg.event === 'sync.completed');
};

function doSubscriptions(ws) {
  console.log('Subscribing to messages...');
  ws.send(JSON.stringify({
          "resource": "_client", 
          "event": "subscribe",
          "data": {
                  "resource": "/storage/synchronizers" 
          }}));
};

function doSync() {
  function onSuccess(responseBody) {
    console.log('onSuccess: Handling - POST /storage/synchronizers, response payload of length - ' + JSON.stringify(responseBody).length);
  };

  function onError(responseBody) {
    console.log('onError: Handling - POST /storage/synchronizers, response payload - ' + JSON.stringify(responseBody));
  };
  var options = {
    onSuccess: onSuccess,
    onError: onError,
    attr: {}
  };

  console.log('Triggering sync...');
  syncResource.doRequest('POST', options);
  console.log('Sync triggered');
};

describe.skip('/storage/synchronizers', function () {

  //This will be called before all tests
  before(function (done) {
    async.waterfall([

      // start local database
      function startLocalDatabase(callback) {
        localDatabaseManager.startDatabase({ execPath: config.db.local.execPath,
                                             port: config.db.local.port,
                                             dbName: config.db.database },
                                           function(err, result) {
                                              if (err) {
                                                console.log(err);
                                              }
                                              else {
                                                console.log("Test database created");
                                              }
                                              callback(null);
                                           });
      },

      // start remote database
      function startRemoteDatabase(callback) {

        remoteDatabaseManager.startDatabase({ host: config.db.remote.host,
                                              port: config.db.remote.port,
                                              dbName: config.db.database,
                                              design_doc: 'couchdb' }, 
                                            function (err, result) {
                                              if (err) {
                                                console.log(err);
                                              }
                                              else {
                                                console.log("Test database created");
                                              }
                                              callback(null);
                                            }
                                           );
      },

      function createSyncResource(callback) {
        var urlVersion = "v0";

        syncResource = new mmApi.StorageSynchronizers('/storage/synchronizers',
                                                      {instName: 'synchronizer',
                                                       pathPrefix: '/' + urlVersion });
        callback(null);
      },

    ], function (err, results) {
      done();
    });
  });//end before

  //
  // Test #1: Do a single sync.
  //
  describe('do a sync', function () {
    it('should get sync.started and sync.completed events', function (done) {

      var ws = new WebSocket('ws://appjs/notifications');

      var gotStarted = false;

      ws.onmessage = function(msg) {
        console.log('Got message - ' + msg.data);
        var parsedMsg = JSON.parse(msg.data);
        if (isConnectionEstablished(parsedMsg)) {
          doSubscriptions(ws);
          doSync();
        }
        else if (isSyncStarted(parsedMsg)) {
          gotStarted.should.equal(false);
          gotStarted = true;
        }
        else if (isSyncCompleted(parsedMsg)) {
          gotStarted.should.equal(true);
          done();
        }
      };
    });
  });

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {

    async.waterfall([

      function destroyRemoteDatabase(callback) {
        remoteDatabaseManager.destroyDatabase(function (err) {
          if (!err) {
            console.log('test database destroyed!');
          }
          callback(null);

        });

      }

    ], function (err) {
      done();
    });
  });//end after

});
