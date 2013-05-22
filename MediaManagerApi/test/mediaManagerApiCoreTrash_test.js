'use strict';

var async = require('async')
  , restify = require('restify')
  , imageService = require('ImageService')
  , testDataManager = require('./TestDataManager')
  , media_manager_api_server = require("../media_manager_api_server")
  , chai = require('chai')
  , expect = chai.expect
  , should = require("should")
  , _ = require('underscore')
  ,log4js = require('log4js');

var thisFileName =  __filename.substring(__filename.lastIndexOf("/"),__filename.length);
var log = log4js.getLogger(thisFileName);


var config = {
  db: {
    database: "plm-media-manager-dev0",
    local: {
      host: "localhost",
      port: 5984
    }
  }
};

var serverPort = 9000;

var dbOptions = {
  host: config.db.local.host,
  port: config.db.local.port,
  dbName: config.db.database,
  dbType: 'couchdb'
};


imageService.config.db.host = dbOptions.host;
imageService.config.db.port = dbOptions.port;
imageService.config.db.name = dbOptions.dbName;


testDataManager.setDBOptions(dbOptions);
testDataManager.setImageService(imageService);


// init the test client
var client = restify.createJsonClient({
  version: '*',
  url: 'http://localhost:' + serverPort
});

function initializeTestServer(options, done) {
  async.waterfall([

    function populateTestData(callback) {

      testDataManager.populateTestData(options, function (err, result) {
        if(err){
          log.error(err);
        }
        else {
          log.info("Test data inserted");
        }
        callback(null);
      });

    },


    //start server
    function startTestServer(callback) {

      media_manager_api_server.startServer(serverPort, config, function (err, result) {
          if(err){
            log.error(err);
          }
          else {
            log.info("Test server started");
          }
          callback(null);
        }

      );
    }

  ], function (err, results) {
    done();
  });
}


function tearDownTestServer(options, done) {
  async.waterfall([

    function stopTestServer(next) {
      media_manager_api_server.stopServer(function (err) {
        if(err){
          log.error(err);
        }else{
          log.info('test server stopped!');
        }
        next(null);

      });

    },
    function destroyTestDatabase(next) {

      testDataManager.destroyTestData(function (err) {
        if(err){
          log.error(err);
        }else{
          log.info('test data destroyed!');
        }
        next(null);

      });


    }

  ], function (err) {
    done();
  });

}

describe('service: MediaManagerApi Trash Operations', function () {


  describe('Trash tests', function () {

    // test: Send an image to the trash
    //PUT /images/<image ID>?in_trash=true

    // Recover an image from the trash
    //PUT /images/<image ID>?in_trash=false

    // Delete permanently an image based on its ID
    // DELETE /images/<image ID>

    // Test  : All images in database must be deleted regardless they are in trash or not
    // DELETE /images[?trashState=any]

    // Empty trash
    // DELETE /images[?trashState=in]


    //Search by trash state
    // indicates whether to return (i) images out of trash, or
    // (ii) all regardless of trash state, or (iii) in trash, respectively.
    // Defaults to trashState=out when parameter is omitted.
    // In other words, by default hide images that have been placed in trash.
    //GET /images?trashState=out|any|in:


    // test: Send an image to the trash
    //PUT /images/<image ID>?in_trash=true

    it('Send an image to the trash', function (done) {

      var imagesOids = [];

      async.waterfall([

        //initialize test images without tags
        function (next) {
          var options = {
            populateTags: false
          };

          initializeTestServer(options, next);
        },

        function (next) {

          // Get the oids of the saved images
          testDataManager.getAllImages(
            function (err, result) {
              if(err){
                log.error(err);
              }
              else {
                imagesOids = _.pluck(result, "oid");

              }
              next(null);
            }

          );
        },

        //pick one of the three images and send it to trash
        function (next) {

          var oidToSendToTrash = imagesOids[0];

          client.put('/v0/images/$' + oidToSendToTrash + '?in_trash=true', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            next();
          });

        },
        //check there is one image in trash
        function (next) {

          client.get('/v0/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            var filteredImages = data.images;
            expect(filteredImages).to.have.length(1);//1 original, the 2 variants are in the variants attribute


            next();
            });

        },
        //restore previous image from trash
        function (next) {

         var oidToSendToTrash = imagesOids[0];

         client.put('/v0/images/$' + oidToSendToTrash + '?in_trash=false', function (err, req, res, data) {

           if(err){
           log.error(err);
           }

           should.not.exist(err);
           res.should.have.status(200);

           next();
         });

         },
        //check there is no image in trash
        function (next) {

          client.get('/v0/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            var filteredImages = data.images;
            expect(filteredImages).to.have.length(0);


            next();
          });

        },
        //pick one of the three images and send it to trash
        function (next) {

          var oidToSendToTrash = imagesOids[0];

          client.put('/v0/images/$' + oidToSendToTrash + '?in_trash=true', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            next();
          });

        },
        //check there is one image in trash
        function (next) {

          client.get('/v0/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            var filteredImages = data.images;
            expect(filteredImages).to.have.length(1);//1 original, the 2 variants are in the variants attribute


            next();
          });

        },
        //Empty the trash
        function (next) {

          client.del('/v0/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);


            next();
          });

        },
        //check there is no image in trash
        function (next) {

          client.get('/v0/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            var filteredImages = data.images;
            expect(filteredImages).to.have.length(0);


            next();
          });

        },
        //pick a single image and delete it permanently
        function (next) {

          var oidToDeletePermanently = imagesOids[1];

          client.del('/v0/images/$' + oidToDeletePermanently, function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            next();
          });

        }
        /*,
        //pick a single image and delete it permanently
        function (next) {

          client.del('/v0/images/$' + imagesOids[0] + '?', function (err, req, res, data) {

           if(err){
           log.error(err);
           }
            should.not.exist(err);
            res.should.have.status(200);


            next();
          });

        }*/


        //restore previous image from trash
        /*function (next) {

          var oidToSendToTrash = imagesOids[0];

          client.put('/v0/images/$' + oidToSendToTrash + '?in_trash=false', function (err, req, res, data) {


           if(err){
           log.error(err);
           }

            should.not.exist(err);
            res.should.have.status(200);

            next();
          });

        },*/
        //check there is no image in trash
        //function (next) {

          //  var oidToSendToTrash = imagesOids[0];

          // client.put('/v0/images/$' + oidToSendToTrash + '?in_trash=false', function (err, req, res, data) {

          //if(err){
           // log.error(err);
          //}

          //   should.not.exist(err);
          /////   res.should.have.status(200);

          //next();
          // });

        //}


      ], function (err, results) {

        done()

      });

    });


    afterEach(function (done) {
      tearDownTestServer(null, done);
    });//end after

  });


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