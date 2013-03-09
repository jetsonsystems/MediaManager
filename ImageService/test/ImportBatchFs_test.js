'use strict';

var async = require("async")
	,config = require("config").db
  ,dbMan = require('./databaseManager.js')
  ,imageService = require('../lib/plm-image/ImageService')
  ,log4js = require('log4js')
  ,nano = require('nano')
  ,util = require('util')
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
describe('ImageService.importBatchFs Setup/Teardown', function () {
  // set a high time-out because it may take a while to run the import
  this.timeout(180000);  // 3 minutes

  imageService.config.db.host = config.local.host;
  imageService.config.db.port = config.local.port;
  var server = nano('http://' + imageService.config.db.host + ':' + imageService.config.db.port);

  var db_name = imageService.config.db.name = config.database;

  var options = {
    host:imageService.config.db.host,
    port:imageService.config.db.port,
    dbName:db_name,
    dbType: config.local.type // couchdb | touchdb
  };

  var 
    db = null
    ,theImportBatch = {}
    ,img_saved_event_count = 0
  ;

  var
    PATH = './test/resources/images'
    ,NUM_TO_IMPORT = 12
  ;

  function assertStarted(importBatch) {
    importBatch.class_name.should.equal('plm.ImportBatch');
    importBatch.oid.should.not.be.empty;
    importBatch.getNumToImport().should.equal(NUM_TO_IMPORT);
    importBatch.getNumAttempted().should.equal(0);
    importBatch.getNumSuccess().should.equal(0);
    importBatch.getNumError().should.equal(0);

    should.exist(importBatch.getCreatedAt());
    should.exist(importBatch.getStartedAt());
    importBatch.getUpdatedAt().should.be.equal(importBatch.getCreatedAt());
    should.not.exist(importBatch.getCompletedAt());
    importBatch.getStatus().should.equal(importBatch.BATCH_STARTED);

  }

  function assertImageSaved(image) {
    image.class_name.should.equal('plm.Image');
    image.oid.should.not.be.empty;
    image.variants.length.should.equal(3);

    if (imageService.config.genCheckSums) {
      image.checksum.should.not.be.empty;
    }
  }

  function assertCompleted(importBatch, anEventCount) {
    importBatch.getNumToImport().should.equal(NUM_TO_IMPORT);
    importBatch.getNumAttempted().should.equal(NUM_TO_IMPORT);
    importBatch.getNumSuccess().should.equal(NUM_TO_IMPORT);
    importBatch.getNumError().should.equal(0);
    should.exist(importBatch.getCreatedAt());
    should.exist(importBatch.getStartedAt());
    should.exist(importBatch.getCompletedAt());
    importBatch.getUpdatedAt().should.be.equal(importBatch.getCompletedAt());
    importBatch.getStatus().should.equal(importBatch.BATCH_COMPLETED);
    anEventCount.should.equal(NUM_TO_IMPORT);
  }


  //This will be called before all tests
  before(function (done) {
    dbMan.startDatabase(options);

    imageService.importBatchFs(
      PATH
      // callback
      ,function(err, importBatch) {
        if (err) console.log("err: %s", err);
        else {
          theImportBatch = importBatch;
          // add listeners
          importBatch.once(importBatch.event.STARTED, function(anEvent) {
            // console.log("event: %s", util.inspect(anEvent));
            console.log("event '%s' emitted at '%s', status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.getStatus());
            assertStarted(anEvent.data);
          });

          // add the image listener
          importBatch.on(importBatch.event.IMG_SAVED, function(anEvent) {
            // console.log("event: %s", util.inspect(anEvent));
            console.log("event '%s' emitted at '%s', status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.getStatus());
            assertImageSaved(anEvent.data);
            img_saved_event_count += 1;
          });

          importBatch.once(importBatch.event.COMPLETED, function(anEvent) {
            console.log("event '%s' emitted at '%s', status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.getStatus());
            assertCompleted(anEvent.data, img_saved_event_count);
            setTimeout(done, 1000);
          });
        }
      }
      //options
      ,{ saveOriginal: true
        ,retrieveSavedImage:true
        ,desiredVariants: [
           { name: 'thumbnail.jpg',  format: "JPG", width: 80,   height: 80}
          ,{ name: 'web.jpg',        format: "JPG", width: 640,  height: 400}
          ,{ name: 'full-small.jpg', format: "JPG", width: 1280, height: 800}
        ]
      }
    );
  });//end before


  //Define the tests

  /**
    */
  it("should have the proper values after it completes", function (done) {
    assertCompleted(theImportBatch, img_saved_event_count);
    done();
  });

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {
    dbMan.destroyDatabase(options, done);
    // done();
  });//end after

});
