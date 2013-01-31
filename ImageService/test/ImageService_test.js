'use strict';

var async = require("async")
  ,dbMan = require('./databaseManager.js')
  ,imageService = require('../lib/plm-image/ImageService')
  ,log4js = require('log4js')
  ,nano = require('nano')
  ,util = require('util')
  ,_    = require('underscore')
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
describe('ImageService Testing', function () {

  imageService.config.db.host = "localhost";
  imageService.config.db.port = 5984;
  var server = nano('http://' + imageService.config.db.host + ':' + imageService.config.db.port);

  var db_name = imageService.config.db.name = 'plm-media-manager-dev0';

  var options = {
    host:imageService.config.db.host,
    port:imageService.config.db.port,
    dbName:db_name,
    design_doc:'couchdb'
  };

  var db = null;

  //This will be called before all tests
  before(function (done) {
    dbMan.startDatabase(options);
    done();
  });//end before

  describe('ImageService.save', function () {

    /*
     Test1 Setup:
     - pick an image path from 'test/resources/images'
     - create an image record in couch using ImageService.save(imagePath)
     - perform various 'Assertions' on the Image object returned by
     save(imagePath), comparing the values of the object to a pre-determined set of
     values in the test (size, format, checksum, oid present, etc...)
     */
    var path_to_images = './test/resources/images';

    var theSavedImage = null;

    before(function (done) {

      // simple save
      imageService.save(
        path_to_images + '/clooney.png',
        function(err, result) {
          // if (err) { console.log(err); throw err; }
          if (err) {
            console.log(err);
            done(err);
          }
          // console.log("result: " + JSON.stringify(result));
          // console.log("inspect: " + util.inspect(result));
          theSavedImage = result;
          done();
        }
      );

    });//end before

    //Define the tests

    /**
     * Each "it" function is a test case
     * The done parameter indicates that the test is asynchronous
     */
    it("The saved image should have some properties", function (done) {

      util.inspect(theSavedImage,true,null,true);

      theSavedImage.name.should.equal('clooney.png');
      theSavedImage.class_name.should.equal('plm.Image');
      theSavedImage.filesize.should.equal('486.3K');
      theSavedImage.format.should.equal('PNG');
      theSavedImage.size.width.should.equal(480);
      theSavedImage.size.height.should.equal(599);
      theSavedImage.geometry.should.equal("480x599");
      expect(theSavedImage.oid).to.be.not.empty;
      expect(theSavedImage.checksum).to.equal("7f69b43f4ef1ff0933b93e14f702bdac");
      theSavedImage._attachments["clooney.png"].content_type.should.equal('image/PNG');
      theSavedImage._attachments["clooney.png"].stub.should.equal(true);
      expect(theSavedImage.variants).to.be.instanceof(Array);
      expect(theSavedImage.variants).to.be.empty;
      expect(theSavedImage.batch_id).to.be.empty;
      expect(theSavedImage.type).to.be.empty;
      expect(theSavedImage.tags).to.be.instanceof(Array);
      expect(theSavedImage.tags).to.be.empty;

      var url = util.format("http://%s:%s/%s/%s/clooney.png", 
        imageService.config.db.host, imageService.config.db.port, imageService.config.db.name, theSavedImage.oid);
      theSavedImage.url.should.equal(url);

      done();
    });

  });

  describe('testing ImageService tags operations', function () {

    /*
    - pick 3 images path from 'test/resources/images'
    - create an image record in couch using ImageService.save(imagePath)
    - add and save tags for them
    - test1: that the retrieved images have the tags in alphabetical order
    - test the findByTags method
    *
    * */

    var path_to_images = './test/resources/images';
    var theSavedImages = {};
    var theRetrievedImages = {};

    var theOriginalTagsMap = {};
    theOriginalTagsMap["eastwood.png"]  = ["trips","family","friends"];
    theOriginalTagsMap["hopper.png"]    = ["zoo","america","boring"];
    theOriginalTagsMap["jayz.png"]      = ["f","l","h"];

    var theExpectedOrderedTagsMap ={};
    theExpectedOrderedTagsMap["eastwood.png"]  = ["family","friends","trips"];
    theExpectedOrderedTagsMap["hopper.png"]    = ["america","boring","zoo"];
    theExpectedOrderedTagsMap["jayz.png"]      = ["f","h","l"];


    before(function (done) {


      var imagesPaths = [path_to_images + '/eastwood.png',
                         path_to_images + '/hopper.png',
                         path_to_images + '/jayz.png'];


      function ingest( anImagePath, next ) {
        imageService.save(
          anImagePath,
          function(err, result) {
            if (err) {
              console.log(err);
              done(err);
            }
            theSavedImages[result.name]=result;
            next();
          }
        );

      }

      async.forEach( imagesPaths, ingest, function(err) {
        if (err) {
          console.log("failed with error %j", err);
          done(err);
        }
        console.log("done!");
        done();
      });


    });//end before

    //Define the tests

    /**
     * Each "it" function is a test case
     * The done parameter indicates that the test is asynchronous
     */
    it("The saved image should have the given tags in alphabetical order", function (done) {


      async.waterfall([

        function saveOrUpdate(callback) {

          _.forEach( _.keys(theSavedImages), function (key) {
            theSavedImages[key].tags_add(theOriginalTagsMap[key]);
          });


          function saveImage( image, next ) {
            imageService.saveOrUpdate(
              image,0,
              function(err, result) {
                if (err) {
                  console.log(err);
                  done(err);
                }

                imageService.show(result.id, function(err, image){
                  if(err){
                    done(err);
                  }else{
                    theRetrievedImages[image.name]=image;
                    next();
                  }
                });


              }
            );

          }

          async.forEach( _.values(theSavedImages), saveImage, function(err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });

        },

        function testTheRetrievedTags(callback) {

          _.forEach( _.keys(theRetrievedImages), function (key) {
            expect(theRetrievedImages[key].tags_get()).to.deep.equal(theExpectedOrderedTagsMap[key]);
          });

          callback(null);
        }

      ], function (err, results) {
        done();
      });

    });//end it

  });//end describe

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {
    dbMan.destroyDatabase( function() {
      done();
    });
  });//end after

});
