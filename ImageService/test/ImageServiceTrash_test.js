'use strict';

var async = require("async")
  , config = require("config").db
  , dbMan = require('./databaseManager.js')
  , imageService = require('../lib/plm-image/ImageService')
  , log4js = require('log4js')
  , nano = require('nano')
  , util = require('util')
  , _ = require('underscore')
  ;

var chai = require('chai')
  , expect = chai.expect
  , should = require("should");

log4js.configure('./test/log4js.json');

/**
 * "describe" function is a container for test cases
 * The functions before and after would be called at the beginning and end of executing a describe block,
 * while beforeEach and afterEach are called before and after each test case of a describe block.
 */
describe('ImageService Testing Trash', function () {

  imageService.config.db.host = config.local.host;
  imageService.config.db.port = config.local.port;

  var db_name = imageService.config.db.name = config.database;

  var options = {
    host: imageService.config.db.host,
    port: imageService.config.db.port,
    dbName: db_name,
    dbType: config.local.type // couchdb | touchdb
  };

  var db = null;

  //This will be called before all tests
  before(function (done) {
    // nothing to do at the moment
    done();
  });//end before

  describe('testing ImageService sending images to trash', function () {

      /*
       - pick 3 images path from 'test/resources/images'
       - save them with 2 variants each
       - send two original images to trash
       - viewTrash should retrieve 6 images
       *
       * */

      var path_to_images = './test/resources/images';
      var theSavedImages = {};

      var imagesToSendToTrashNames = ["eastwood.png", "hopper.png"];


      beforeEach(function (done) {

        //create test database
        dbMan.startDatabase(options);


        var imagesPaths = [path_to_images + '/eastwood.png',
          path_to_images + '/hopper.png',
          path_to_images + '/jayz.png'];


        function ingest(anImagePath, next) {
          var saveOptions =
          { retrieveSavedImage: true,
            saveOriginal: true,
            desiredVariants: [
              {   name: 'thumb.jpg', format: "JPG", width: 120, height: 150}
              ,
              {  name: 'screen.jpg', format: "JPG", width: 360, height: 450}
            ]
          };
          imageService.save(
            anImagePath,
            saveOptions,
            function (err, result) {
              if (err) {
                console.log(err);
                done(err);
              }
              theSavedImages[result.name] = result;
              next();
            }
          );

        }


        async.waterfall([

          function saveImagesWithAttachments(next) {
            async.forEach(imagesPaths, ingest, function (err) {
              if (err) {
                console.log("failed with error %j", err);
                done(err);
              }
              console.log("done!");
              next();
            });
          },

          function sendImagesToTrash(next) {

            var oidOfImagesToSendToTrash = [];

            _.forEach(imagesToSendToTrashNames, function (imageToSendToTrashName) {
              oidOfImagesToSendToTrash.push(theSavedImages[imageToSendToTrashName].oid);
            });

            imageService.sendToTrash(oidOfImagesToSendToTrash, function (err) {
                if (err) {
                  console.log("failed with error %j", err);
                  done(err);
                }
                console.log("done!");
                next();
              }


            );


          }
        ], function (err, results) {
          done();
        });


      });//end before

      //Define the tests

      /**
       * Each "it" function is a test case
       * The done parameter indicates that the test is asynchronous
       */
      it("The images sent to trash should have deleted equals true", function (done) {

        imageService.viewTrash(null, function (err, docs) {
          if (err) {
            done(err);
          } else {
            expect(docs).to.have.length(6);//2 original images plus their 2 variants each

          }

          _.each(docs,function(doc){
              if(doc.isOriginal()){
                expect(doc.variants).to.have.length(2);//if is original should have 2 variants
              }
            });

          done();
        });

      });


      /**
       * Each "it" function is a test case
       * The done parameter indicates that the test is asynchronous
       */
      it("After emptying the trash, the VIEW trash should not return any document", function (done) {

        imageService.emptyTrash(function(err){
          if (err) {
            log.error("Error while emptying trash", err);
            done(err);
          } else {

            imageService.viewTrash(null, function (err, docs) {
                if (err) {
                  done(err);
                }
                expect(docs).to.have.length(0);
                done();

              });
          }
        });

      });

      afterEach(function (done) {
        dbMan.destroyDatabase(options, done);
      });//end after
    }
  );


  /*after(function (done) {
    dbMan.destroyDatabase(options, done);
  });//end after
*/

});//end describe

