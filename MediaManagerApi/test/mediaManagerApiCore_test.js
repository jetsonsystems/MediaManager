'use strict';

var async = require('async')
  ,restify = require('restify')
  ,should = require('should')
  ,media_manager_api_server = require("./media_manager_api_server")
  ,databaseManager = require("./databaseManager");


var dbOptions = {
   host : "localhost"
  ,port : 5984
  ,dbName : "plm-media-manager-dev0"
  ,design_doc:'couchdb'
}

var serverOptions = {
  host : "localhost"
  ,port : 9000
  ,dbOptions : dbOptions
}


// init the test client
var client = restify.createJsonClient({
  version:'*',
  url:'http://'+serverOptions.host +':'+serverOptions.port
});



describe('service: MediaManagerApi', function () {

  //This will be called before all tests
  before(function (done) {
    async.waterfall([

      //create test database
      function startTestDatabase(callback) {

        databaseManager.startDatabase(dbOptions, function (err, result) {
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

      //start server
      function startTestServer(callback) {

        media_manager_api_server.startServer(serverOptions, function (err, result) {
            if (err) {
              console.log(err);
            }
            else {
              console.log("Test server started");
            }
            callback(null);
          }

        );
      }

    ], function (err, results) {
      done();
    });
  });//end before


  //http://localhost:9000/v0/images
  // Test #1
  describe('200 response check', function () {
    it('should get a 200 response', function (done) {
      client.get('/v0/images', function (err, req, res, data) {

        should.not.exist(err);
        res.should.have.status(200);

        done();
      });
    });
  });

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {

    async.waterfall([

      function stopTestServer(callback) {
        media_manager_api_server.stopServer(function (err) {
          if (!err) {
            console.log('test server stopped!');
          }
          callback(null);

        });

      },
      function destroyTestDatabase(callback) {
        databaseManager.destroyDatabase(function (err) {
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



  //-------------------------------------------------------------------------------------------------------------------
  //REQUEST:
  //method POST
  //localhost:9000/v0/importers
  //import_dir    /home/dmora/projects/jetsonsys/MediaManager/MediaManagerApi/test/images
  //RESPONSE
  /*
   {
   "status": 0,
   "importer": {
   "id": "$73fabdf0-fed1-4423-83fd-ff5d6b5e3ee6",
   "import_dir": "/home/dmora/projects/jetsonsys/MediaManager/MediaManagerApi/test/images",
   "created_at": "2012-12-21T16:37:10.804Z",
   "started_at": "2012-12-21T16:37:10.804Z",
   "num_to_import": 1,
   "num_imported": 0,
   "num_success": 0,
   "num_error": 0
   }
   }
   */
  //-------------------------------------------------------------------------------------------------------------------


});