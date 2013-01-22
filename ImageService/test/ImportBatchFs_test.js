'use strict';

var async = require("async")
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
  // set a one minute time-out because it may take a while to run the import
  this.timeout(60000);

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

  function assertStarted(anEvent) {
    var importBatch = anEvent.data;

    console.log("importBatch event emitted at '%s', status is: '%s'", anEvent.emitted_at, importBatch.getStatus());
  }

  function assertCompleted(anEvent) {
    var importBatch = anEvent.data;

    console.log("importBatch event emitted at '%s', status is: '%s'", anEvent.emitted_at, importBatch.getStatus());
  }


  //This will be called before all tests
  before(function (done) {
    dbMan.startDatabase(options);
    done();
  });//end before

  describe('ImageService.importBatchFs', function () {

    var PATH = './test/resources/images';

    var theImportBatch = {};

    before(function (done) {

      // simple save
      imageService.importBatchFs(
        PATH
        // callback
        ,function(err, importBatch) {
          if (err) console.log("err: %s", err);
          else {
            theImportBatch = importBatch;
            // add listeners
            importBatch.once(importBatch.event.STARTED, assertStarted);
            importBatch.once(importBatch.event.COMPLETED, function(anEvent) {
              assertCompleted(anEvent);
              setTimeout(done, 1000);
            });
          }
        }
        //options
        ,{ saveOriginal: true
          ,desiredVariants: [
             { name: 'thumb.jpg',  format: "JPG", width: 120, height: 150}
            ,{ name: 'screen.jpg', format: "JPG", width: 360, height: 450}
          ]
        }
      );

    });//end before

    //Define the tests

    /**
     * Each "it" function is a test case
     * The done parameter indicates that the test is asynchronous
     */
    it("The saved image should have some properties", function (done) {

      // console.log("theImportBatch: %s", util.inspect(theImportBatch,true,null,true));

      done();
    });

  });
  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {

    done();
    /*
    dbMan.destroyDatabase( function() {
      done();
    });
    */
  });//end after

});
