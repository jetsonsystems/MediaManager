'use strict';

var async = require('async')
  , restify = require('restify')
  , config = require('./config')
  , imageService = require('ImageService')
  , testDataManager = require('./TestDataManager')
  , media_manager_api_server = require("../media_manager_api_server")
  , chai = require('chai')
  , expect = chai.expect
  , should = require("should")
  , _ = require('underscore')
  , log4js = require('log4js');

var thisFileName = __filename.substring(__filename.lastIndexOf("/"), __filename.length);
var log = log4js.getLogger(thisFileName);


var serverPort = 9000;

var dbOptions = {
  host: config.db.local.host,
  port: config.db.local.port,
  dbName: config.db.database,
  dbType: config.db.local.type // couchdb | touchdb
};



imageService.config.db.host = dbOptions.host;
imageService.config.db.port = dbOptions.port;
imageService.config.db.name = dbOptions.dbName;


testDataManager.setDBOptions(dbOptions);
testDataManager.setImageService(imageService);


// init the test client
var client = restify.createJsonClient({
  version: '*',
  url: 'http://localhost:' + serverPort,
  headers: {connection: "close"}
});

function initializeTestServer(options, done) {
  async.waterfall([

    function populateTestData(callback) {

      testDataManager.populateTestData(options, function (err, result) {
        if (err) {
          if(err){
            log.error(err);
          }
        }
        else {
          log.info("Test data inserted");
        }
        callback(null);
      });

    },


    //start server
    function startTestServer(callback) {

      media_manager_api_server.startServer(serverPort,config, function (err, result) {
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

describe('service: MediaManagerApi', function () {


  //http://localhost:9000/v0/images
  // Test #1
  describe('Retrieving Images', function () {

    //This will be called before each test
    beforeEach(function (done) {
      var options = {
        populateTags:true
      };
      initializeTestServer(options, done);

    });//end beforeEach

    it('should get all the three images', function (done) {
      client.get('/v0/images?', function (err, req, res, data) {

        if(err){
          log.error(err);
        }

        should.not.exist(err);
        res.should.have.status(200);

        var filteredImages = data.images;
        expect(filteredImages).to.have.length(3);

        done();
      });
    });

    it('should get 2 images containing tag family', function (done) {
      client.get('/v0/images?tags=family', function (err, req, res, data) {

        if(err){
          log.error(err);
        }

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

        if(err){
          log.error(err);
        }

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

        if(err){
          log.error(err);
        }

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

    afterEach(function (done) {
      tearDownTestServer(null,done);
    });//end afterEach
  });

  describe('Retrieving Tags', function () {
    //This will be called before each test
    beforeEach(function (done) {

      var options = {
        populateTags:true
      };
      initializeTestServer(options, done);

    });//end beforeEach


    var theExpectedListOfDistinctTags = ["america", "family", "friends", "trips", "zoo"];

    it('should get all the distinct tags in database', function (done) {
      client.get('/v0/tags?', function (err, req, res, data) {

        if(err){
          log.error(err);
        }

        should.not.exist(err);
        res.should.have.status(200);

        var listOfAllTagsInDatabase = data.tags;
        expect(listOfAllTagsInDatabase).to.deep.equal(theExpectedListOfDistinctTags);

        done();
      });
    });


    it('Get the tags of a set of images', function (done) {
      /**
       * "eastwood.png" = ["trips", "family", "friends"];
       * "hopper.png" = ["zoo", "america", "friends"];
       */
      var expectedTagsForEastWoodAndHopper = ["america", "family", "friends", "trips", "zoo"];

      var imagesIds = {};


      async.waterfall([

        function (next) {
          //get the ids of the images
          client.get('/v0/images?', function (err, req, res, data) {

            if (err) {
              log.error(err);
            }
            _.each(data.images,function(image){
                imagesIds[image.name]=image.id;
            });

            next();
          });

        },
        //retrieve the tags for of eastwood.png and hopper.png
        function (next) {

          //eastwood.png id (already has the $ prefix)
          var estwoodId = imagesIds["eastwood.png"];

          //hopper.png id (already has the $ prefix)
          var hopperId = imagesIds["hopper.png"];

          client.get('/v0/tags?images='+estwoodId+','+hopperId, function (err, req, res, data) {
            if (err) {
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            var retrievedListOfTagsOfListOfImages = data.tags;
            expect(retrievedListOfTagsOfListOfImages).to.deep.equal(expectedTagsForEastWoodAndHopper);

            next()
          });
        }

      ], function (err, results) {
        done();
      });


    });

    afterEach(function (done) {
      tearDownTestServer(null, done);
    });//end afterEach


  });

  describe('Tagger tests', function () {


    /*beforeEach(function (done) {
     var options = {
     populateTags:false
     };
     initializeTestServer(options, done);
     });//end beforeEach*/


    /*{
     <action> : {
     "images" : "[id1, id2, ...]"
     ,"tags"  : "[tag1, tag2, ...]
     }
     }*/

    //Test 1
    //Assigns a list of tags to a list of Images, with the assumption that the Images have not been tagged yet.

    it('Assigns a list of tags to a list of Images', function (done) {

      var imagesOids = [];

      async.waterfall([

        //initialize test images without tags
        function (next) {
          var options = {
            populateTags:false
          };

          initializeTestServer(options, next);
        },

        function (next) {

          // Get the oids of the saved images
          testDataManager.getAllImages(
            function (err, result) {
              if(err){
                log.error(err);
              }else {
                imagesOids = _.pluck(result, "oid");
              }
              next(null);
            }

          );
        }
      ], function (err, results) {

        var imagesOidsWith$ = [];
        //prepend a $ on each imageOid
        _.forEach(imagesOids, function (imageOid) {
          imagesOidsWith$.push('$'+imageOid);
        });

        //add the tags to the images

        var assignTagsCommand = {
          "add":{
            "images":imagesOidsWith$, "tags":["tag3", "tag2", "tag1"]
          }
        };

        client.post('/v0/tagger?', assignTagsCommand, function (err, req, res, data) {

          if(err){
            log.error(err);
          }

          //assert that the tags were added to the images

          should.not.exist(err);
          res.should.have.status(200);

          var modifiedImages = data.tagger;
          expect(modifiedImages).to.have.length(3);

          //TODO: the response must contain the images updated with the added tags

          done();
        });

      });

    });

    //Test
    it('Replace a list of tags in a list of Images', function (done) {

      var imagesOids = [];

      async.waterfall([

        //initialize test images with tags
        function (next) {
          var options = {
            populateTags:true
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
        }
      ], function (err, results) {

        /**
         *
         * The tags in oldTags will be replaced by the tags in newTags
         * oldTags[1] will be replaced by newTags[1]
         * oldTags[2] will be replaced by newTags[2]
         *          .
         *          .
         * oldTags[n] will be replaced by newTags[n]
         */

        var imagesOidsWith$ = [];
        //prepend a $ on each imageOid
        _.forEach(imagesOids, function (imageOid) {
          imagesOidsWith$.push('$'+imageOid);
        });

        var replaceTagsCommand = {
          "replace":{
            "images":imagesOidsWith$
            ,"oldTags":["family", "friends"]
            ,"newTags":["my family", "my friends"]
          }
        };

        client.post('/v0/tagger?', replaceTagsCommand, function (err, req, res, data) {

          if(err){
            log.error(err);
          }

          //assert that the tags were replaced in the images

          should.not.exist(err);
          res.should.have.status(200);

          var modifiedImages = data.tagger;
          expect(modifiedImages).to.have.length(3);

          //TODO: the response must contain the images updated with the replaced tags

          done();
        });

      });

    });


    /*
     * remove: removes a list of tags from the tags of a list of Images.
     * This action removes the tags passed from the list of tags that an Image may already have. For example,
     * removing the tags ["red","white","blue"] from an image that has the tags ["blue","green","yellow"]
     * would result in the image having the tags: ["green", "yellow"].
     * */
    it('Remove a list of tags in a list of Images', function (done) {

      var imagesOids = [];

      async.waterfall([

        //initialize test images with tags
        function (next) {
          var options = {
            populateTags:true
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
        }
      ], function (err, results) {

        var imagesOidsWith$ = [];
        //prepend a $ on each imageOid
        _.forEach(imagesOids, function (imageOid) {
          imagesOidsWith$.push('$'+imageOid);
        });

        var removeTagsCommand = {
          "remove":{
            "images":imagesOidsWith$
            ,"tagsToRemove":["family", "friends"]
          }
        };

        client.post('/v0/tagger?', removeTagsCommand, function (err, req, res, data) {

          if(err){
            log.error(err);
          }

          //assert that the tags were removed in the images

          should.not.exist(err);
          res.should.have.status(200);

          var modifiedImages = data.tagger;
          expect(modifiedImages).to.have.length(3);

          //TODO: the response must contain the images updated with the removed tags

          done();
        });

      });

    });


    afterEach(function (done) {
      tearDownTestServer(null,done);
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