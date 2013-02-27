'use strict';

var async = require("async")
    ,nano = require('nano')
    ,util = require('util')
    ,updateDesignDoc = require('../../ImageService/test/update_design_doc');

var chai = require('chai')
    , expect = chai.expect
    , should = require("should");


var server = null;
var db = null;
var dbName=null

exports.startDatabase = startDatabase;
exports.destroyDatabase = destroyDatabase;


function startDatabase(options, callback) {

  var dbHost=options.host;
  var dbPort=options.port;
  dbName = options.dbName;
  var design_doc=options.design_doc;

  server = nano('http://' + dbHost + ':' + dbPort);

   async.waterfall([

    function checkIfTestDatabaseExists(next) {
      var existsTestDatabase = false;
      //check if the testing database exists by getting information about database
      server.db.get(dbName, function (err, body) {
          if (!err) {
            existsTestDatabase = true;
          }
          next(null, existsTestDatabase);
        }
      );
    },

    function deleteTestDatabase(existsTestDatabase, next) {
      if (existsTestDatabase) {
        console.log('Attempting to destroy existing test database: ' + dbName);
        server.db.destroy(dbName, function (err, body) {
          if (!err) {
            console.log('Existing test database ' + dbName + '  was destroyed');
          }
          next();
        });

      } else {
        next();
      }
    },

    function createTestDatabase(next) {
      server.db.create(dbName, function (err, body) {
        if (!err) {
          console.log('database ' + dbName + '  created!');
        }
        next(null);

      });

    },
    function insertDesignDocs(next) {

      updateDesignDoc.updateDesignDoc(options,function(err,result){
          if(err){
            console.log(err);
          }
          else{
            console.log("Updated Design Doc: " + util.inspect(result,true,null,true));
          }
          next(null);
        }

      );
    }

  ], function (err, results) {
    db = server.use(dbName);
    callback(null);
  });

}


function destroyDatabase(callback) {

  async.waterfall([

    function destroyDatabase(next) {
      server.db.destroy(dbName, function (err, body) {
        if (!err) {
          console.log('database ' + dbName + '  destroyed!');
        }
        next(null);

      });

    }
  ], function (err, results) {
    callback(null);
  });

}



