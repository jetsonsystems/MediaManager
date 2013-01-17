'use strict';

var async = require("async")
  ,nano = require('nano')
  ,util = require('util')
  ,updateDesignDoc = require('./update_design_doc');

var chai = require('chai')
  , expect = chai.expect
  , should = require("should");


var server = null;
var db = null;
var dbName=null

exports.startDatabase   = startDatabase;
exports.destroyDatabase = destroyDatabase;

function startDatabase(options, callback) {

  var dbHost=options.host;
  var dbPort=options.port;
  dbName = options.dbName;
  var design_doc=options.design_doc;

  server = nano('http://' + dbHost + ':' + dbPort);

  async.waterfall([

    function checkIfTestDatabaseExists(callback) {
      var existsTestDatabase = false;
      //check if the testing database exists by getting information about database
      server.db.get(dbName, function (err, body) {
        if (!err) {
          existsTestDatabase = true;
        }
          callback(null, existsTestDatabase);
      }
      );
    },

    function deleteTestDatabase(existsTestDatabase, callback) {
      if (existsTestDatabase) {
        console.log('Attempting to destroy existing test database: ' + dbName);
        server.db.destroy(dbName, function (err, body) {
        if (!err) {
          console.log('Existing test database ' + dbName + '  was destroyed');
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
          console.log('database ' + dbName + '  created!');
        }
        callback(null);
 
      });
    },

    function insertDesignDocs(callback) {

      updateDesignDoc.updateDesignDoc(options,function(err,result){
        if(err){
          console.log(err);
        }
        else{
          console.log("Updated Design Doc: " + util.inspect(result,true,null,true));
        }
          callback(null);
      });
    }
  ], function (err, results) {
    db = server.use(dbName);
    if (callback) {
      callback(err, results);
    }
  });

}


function destroyDatabase(callback) {

  async.waterfall([
    function destroyDatabase(next) {
      // console.log("attempting to destroy db:'%s'", dbName);
      server.db.destroy(dbName, function (err, body) {
        if (!err) {
          console.log('database ' + dbName + '  destroyed!');
          next(null);
        } else {
          next(err, body);
        }
      });

    }
  ], 
  function (err, results) {
    if (err) console.log("Error destroying db %s", err);
    if (callback) {
      callback(err, results);
    }
  });
}
