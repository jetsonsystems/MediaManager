'use strict';

var async = require('async')
  , restify = require('restify')
  , config = require('config')
  , imageService = require('ImageService')
  , dbMan  = require('./databaseManager.js')
  , media_manager_api_server = require("../media_manager_api_server")
  , chai = require('chai')
  , expect = chai.expect
  , should = require("should")
  , _ = require('underscore')
  , log4js = require('log4js');

var thisFileName =  __filename.substring(__filename.lastIndexOf("/"),__filename.length);
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


// init the test client
var client = restify.createJsonClient({
  version: '*',
  url: 'http://localhost:' + serverPort,
  headers: {connection: "close"}
});

function initializeTestServer(done) {
  async.waterfall([


    function startDatabase(next) {
      dbMan.startDatabase(dbOptions, function (err, result) {
        if(err){
          log.error(err);
        }
        else {
          log.info("Test database started");
        }
        next(null);
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


      dbMan.destroyDatabase(function (err) {
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

describe('service: MediaManagerApi ImportBatch Operations', function () {


  describe('Create ImportBatch tests', function () {


    // Create an import batch
    // Retrieve the images of the import batch
    // Send one image of the import batch to the trash
    // Retrieve the images of the import batch, it should not contain the image sent to trash
    // Restore the image from trash
    // Retrieve the images of the import batch, it should contain the image retrieved from trash

    it('create import batch', function (done) {

      var importBatchOid;
      var importBatchImagesOids = [];
      var oidImageToSendToTrash;

      async.waterfall([

        function (next) {
          initializeTestServer(next);
        },

        //create an import
        function (next) {

          var createImportBatchCommand = {
            "import_dir": "./test/resources/images"
          };

          client.post('/v0/importers', createImportBatchCommand, function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            importBatchOid =  data.importer.id.replace('$', '');

            should.not.exist(err);
            res.should.have.status(200);

            log.info("waiting 15 seconds for the import to complete");
            setTimeout(next, 15000);

          });

        },


      //retrieve the images of the importbatch
      //Pick up one
      function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            oidImageToSendToTrash=importBatchImagesOids[0];

            next();
          });

      },

      // send the image to the trash
      function (next) {

        client.put('/v0/images/$' + oidImageToSendToTrash + '?in_trash=true', function (err, req, res, data) {

          if(err){
            log.error(err);
          }

          should.not.exist(err);
          res.should.have.status(200);

          next();
        });
      },

      // Retrieve the images of the import batch, it should not contain the image sent to trash
        function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images?trashState=out', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            expect(importBatchImagesOids).to.have.length(3);

            //the sent to trash image should not be present
            expect(importBatchImagesOids).to.not.contain(oidImageToSendToTrash);

            next();
          });

        },
        // Retrieve the images of the import batch, do not use the trashState parameter
        // By default should retrieve the images of the importbatch that are not in trash
        function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            expect(importBatchImagesOids).to.have.length(3);

            //the sent to trash image should not be present
            expect(importBatchImagesOids).to.not.contain(oidImageToSendToTrash);

            next();
          });

        },
        // Retrieve the images of the import batch that are in trash, it should contain the image sent to trash
        function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            expect(importBatchImagesOids).to.have.length(1);

            //the sent to trash image should be present
            expect(importBatchImagesOids).to.contain(oidImageToSendToTrash);

            next();
          });

        },

        // Retrieve the images of the import batch regardless of the trash state, it should contain all the images
        function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images?trashState=any', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            expect(importBatchImagesOids).to.have.length(4);

            //the image sent to trash should be present
            expect(importBatchImagesOids).to.contain(oidImageToSendToTrash);

            next();
          });

        },
        // restore the image from trash
        function (next) {

          client.put('/v0/images/$' + oidImageToSendToTrash + '?in_trash=false', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            next();
          });
        },
        // Retrieve the images of the import batch that are in trash, it should be empty
        function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images?trashState=in', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            expect(importBatchImagesOids).to.have.length(0);

            next();
          });

        },
        // Retrieve the images of the import batch that are out of trash, it should contain all the images
        function (next) {

          client.get('/v0/importers/$'+importBatchOid+'/images?trashState=out', function (err, req, res, data) {

            if(err){
              log.error(err);
            }

            should.not.exist(err);
            res.should.have.status(200);

            importBatchImagesOids = _.pluck(data.importer.images, 'id');
            //remove prefix $
            for (var i = 0; i < importBatchImagesOids.length; i++) {
              importBatchImagesOids[i]=importBatchImagesOids[i].replace('$', '');
            }
            expect(importBatchImagesOids).to.have.length(4);

            //the image restored from trash image should be present
            expect(importBatchImagesOids).to.contain(oidImageToSendToTrash);

            next();
          });

        }

      ], function (err, results) {

        done()

      });

    });


    afterEach(function (done) {
      tearDownTestServer(null, done);
    });//end after

  });

});