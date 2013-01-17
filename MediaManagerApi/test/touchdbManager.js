'use strict';

var async = require("async")
    ,nano = require('nano')
    ,util = require('util');

var storage = require('MediaManagerAppSupport/lib/storage.js');

var dataStore = null;
var dbName=null

exports.startDatabase = startDatabase;
exports.destroyDatabase = destroyDatabase;

//
// startDatabase:
//
//  options:
//    execPath
//    port
//    dbName
//
function startDatabase(options, callback) {

  var server = null;
  var dbPort=options.port;
  dbName = options.dbName;

  async.waterfall([

    function launchTouchdb(callback) {
      dataStore = storage.init({db: {
        database: dbName,
        local: {
          execPath: options.execPath,
          port: dbPort
        }
      }});

      console.log('Initialized touchdb...');
  
      dataStore.on('localStorageExit', function() {
        throw 'localStorageExit';
      });

      dataStore.on('localStorageShutdown', function() {
        throw 'localStorageShutdown';
      });

      //
      // Somewhat of a hack. Sometimes the server just ain't ready yet.
      //
      function checkConnection(onReady) {

        function checkOnce() {
          var status = 0;

          async.waterfall([
            function connectAndTest(cb) {
              try {
                server = nano('http://localhost:' + dbPort);
                server.request({db: dbName},
                               function(err, body) {
                                 cb(err, body);
                               });
              }
              catch (err) {
                console.log('TouchDB connection error - ' + err);
                cb(err, undefined);
                status = 1;
              }
            }], function (err, body) {
              if (err) {
                status = 1;
              }
            });
          return status;
        };

        if (checkOnce()) {
          setTimeout(checkOnce, 0.1);
        }
        else {
          onReady();
        }
      };

      checkConnection(function() { callback(); });

    },

    function checkIfTestDatabaseExists(callback) {
      var existsTestDatabase = false;
      //check if the testing database exists by getting information about database
      server.db.get(dbName, function (err, body) {
        if (err) {
          console.log('TouchDB test database - ' + dbName + ', DOES NOT exist, err - ' + err + '!');
        }
        else {
          console.log('TouchDB test database - ' + dbName + ', already exists!');
          existsTestDatabase = true;
        }
        callback(null, existsTestDatabase);
      });
    },

    function deleteTestDatabase(existsTestDatabase, callback) {
      if (existsTestDatabase) {
        console.log('Attempting to destroy existing TouchDB test database: ' + dbName);
        server.db.destroy(dbName, function (err, body) {
          if (!err) {
            console.log('Existing TouchDB test database ' + dbName + '  was destroyed');
          }
          else {
            console.log('Error destroyoing existing TouchDB test database ' + dbName);
          }
          callback();
        });

      } else {
        callback();
      }
    },

    function createTestDatabase(callback) {
      server.db.create(dbName, function (err, body) {
        if (!err) {
          console.log('TouchDB database ' + dbName + '  created!');
        }
        callback(null);

      });

    }

  ], function (err, results) {
    callback(null);
  });

}

function destroyDatabase(callback) {

  async.waterfall([

    function destroyDatabase(callback) {
      server.db.destroy(dbName, function (err, body) {
        if (!err) {
          console.log('database ' + dbName + '  destroyed!');
        }
        callback(null);

      });

    }
  ], function (err, results) {
    callback(null);
  });

}
