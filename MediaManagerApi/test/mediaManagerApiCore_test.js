'use strict';

var async = require('async')
  ,restify = require('restify')
  ,imageService = require('ImageService')
  ,testDataManager = require('./TestDataManager')
  ,media_manager_api_server = require("./media_manager_api_server")
  ,chai = require('chai')
  ,expect = chai.expect
  ,should = require("should")
  , _ = require('underscore');


var dbOptions = {
  host : "localhost"
  ,port : 5984
  ,dbName : "plm-media-manager-dev0"
  ,design_doc:'couchdb'
}


imageService.config.db.host = dbOptions.host;
imageService.config.db.port = dbOptions.port;
imageService.config.db.name = dbOptions.dbName;


var serverOptions = {
  host : "localhost"
  ,port : 9000
  ,dbOptions : dbOptions
};

testDataManager.setImageService(imageService);


// init the test client
var client = restify.createJsonClient({
  version:'*',
  url:'http://'+serverOptions.host +':'+serverOptions.port
});



describe('service: MediaManagerApi', function () {

  //This will be called before all tests
  before(function (done) {
    async.waterfall([

      function populateTestData(callback) {

        testDataManager.populateTestData(function (err, result) {
          if (err) {
            console.log(err);
          }
          else {
            console.log("Test data inserted");
          }
          callback(null);
        });

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

    it('should get all the three images', function (done) {
      client.get('/v0/images?', function (err, req, res, data) {

        should.not.exist(err);
        res.should.have.status(200);

        var filteredImages = data.images;
        expect(filteredImages).to.have.length(3);

        done();
      });
    });

    it('should get 2 images containing tag family', function (done) {
      client.get('/v0/images?tags=family', function (err, req, res, data) {

        should.not.exist(err);
        res.should.have.status(200);

        var filteredImages = data.images;
        expect(filteredImages).to.have.length(2);
        var resultNames = _.pluck(filteredImages, "name");
        expect(resultNames).to.contain("eastwood.png");
        expect(resultNames).to.contain("jayz.png");

        done();
      });
    });

    it('should get 1 images containing tag "america"', function (done) {
      client.get('/v0/images?tags=america', function (err, req, res, data) {

        should.not.exist(err);
        res.should.have.status(200);

        var filteredImages = data.images;
        expect(filteredImages).to.have.length(1);
        var resultNames = _.pluck(filteredImages, "name");
        expect(resultNames).to.contain("hopper.png");

        done();
      });
    });


    it('should get 2 images containing tag "family" AND tag "friend"', function (done) {
      client.get('/v0/images?tags=family,friends&tag_query_op=AND', function (err, req, res, data) {

        should.not.exist(err);
        res.should.have.status(200);

        var filteredImages = data.images;
        expect(filteredImages).to.have.length(2);
        var resultNames = _.pluck(filteredImages, "name");
        expect(resultNames).to.contain("eastwood.png");
        expect(resultNames).to.contain("jayz.png");

        done();
      });
    });


  });

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {

    async.waterfall([

      function stopTestServer(next) {
        media_manager_api_server.stopServer(function (err) {
          if (!err) {
            console.log('test server stopped!');
          }
          next(null);

        });

      },
      function destroyTestDatabase(next) {

        testDataManager.destroyTestData(function (err) {
          if (!err) {
            console.log('test data destroyed!');
          }
          next(null);

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