'use strict';

var async  = require("async")
	, config = require("config").db
  , dbMan  = require('./databaseManager.js')
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
describe('ImageService Testing Tags', function () {

  imageService.config.db.host = config.local.host;
  imageService.config.db.port = config.local.port;
  var server = nano('http://' + imageService.config.db.host + ':' + imageService.config.db.port);

  var db_name = imageService.config.db.name = config.database;

  var options = {
    host:imageService.config.db.host,
    port:imageService.config.db.port,
    dbName:db_name,
		dbType: config.local.type // couchdb | touchdb
    // design_doc:'couchdb'
  };

  var db = null;

  //This will be called before all tests
  before(function (done) {
    // nothing to do at the moment
    done();
  });//end before

  describe('testing ImageService finding by tags', function () {

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
    theOriginalTagsMap["eastwood.png"] = ["trips", "family", "friends"];
    theOriginalTagsMap["hopper.png"] = ["zoo", "america", "friends"];
    theOriginalTagsMap["jayz.png"] = ["f", "l", "family", "friends"];

    var theExpectedOrderedTagsMap = {};
    theExpectedOrderedTagsMap["eastwood.png"] = ["family", "friends", "trips"];
    theExpectedOrderedTagsMap["hopper.png"] = ["america", "friends", "zoo"];
    theExpectedOrderedTagsMap["jayz.png"] = ["f", "family", "friends", "l"];


    before(function (done) {

      //create test database
      dbMan.startDatabase(options);

     
      var imagesPaths = [path_to_images + '/eastwood.png',
        path_to_images + '/hopper.png',
        path_to_images + '/jayz.png'];


      function ingest(anImagePath, next) {
        var saveOptions={retrieveSavedImage:true};
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

        function saveImagesWithAttachments(callback) {
          async.forEach(imagesPaths, ingest, function (err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });
        },

        function updateImagesWithTheTags(callback) {

          _.forEach(_.keys(theSavedImages), function (key) {
            theSavedImages[key].tagsAdd(theOriginalTagsMap[key]);
          });


          function updateImage(image, next) {
            imageService.saveOrUpdate(
              {"doc":image, "tried":0},
              function (err, result) {
                if (err) {
                  console.log(err);
                  done(err);
                }

                imageService.show(result.id, null, function (err, image) {
                  if (err) {
                    done(err);
                  } else {
                    theRetrievedImages[image.name] = image;
                    next();
                  }
                });


              }
            );

          }

          async.forEach(_.values(theSavedImages), updateImage, function (err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });

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
    it("The saved images should have the given tags in alphabetical order", function (done) {

        _.forEach(_.keys(theRetrievedImages), function (key) {
          expect(theRetrievedImages[key].tagsGet()).to.deep.equal(theExpectedOrderedTagsMap[key]);
        });
        done();
      }
    );

    it("searching by Tags with AND and OR", function (done) {

      async.waterfall([

        function searchWithAND(callback) {

          var filterByTag = {
            "groupOp":"AND",
            "rules":[
              {
                "field":"tags",
                "op":"eq",
                "data":"friends"
              },
              {
                "field":"tags",
                "op":"eq",
                "data":"family"
              }
            ]
          };

          var filteredImages = null;

          imageService.findByTags(filterByTag, null, function (err, images) {
            if (err) {
              done(err);
            } else {
              filteredImages = images;
              expect(filteredImages).to.have.length(2);
              var resultNames = _.pluck(filteredImages, "name");
              expect(resultNames).to.contain("eastwood.png");
              expect(resultNames).to.contain("jayz.png");

              callback();
            }
          });

        },
        function anotherSearchWithAND(callback) {

          var filterByTag = {
            "groupOp":"AND",
            "rules":[
              {
                "field":"tags",
                "op":"eq",
                "data":"america"
              },
              {
                "field":"tags",
                "op":"eq",
                "data":"trips"
              }
            ]
          };

          var filteredImages = null;

          imageService.findByTags(filterByTag, null, function (err, images) {
            if (err) {
              done(err);
            } else {
              filteredImages = images;
              expect(filteredImages).to.have.length(0);
              callback();
            }
          });

        },
        function searchWithOR(callback) {

          var filterByTag = {
            "groupOp":"OR",
            "rules":[
              {
                "field":"tags",
                "op":"eq",
                "data":"friends"
              },
              {
                "field":"tags",
                "op":"eq",
                "data":"family"
              }
            ]
          };

          var filteredImages = null;

          imageService.findByTags(filterByTag, null, function (err, images) {
            if (err) {
              done(err);
            } else {
              filteredImages = images;
              expect(filteredImages).to.have.length(3);
              var resultNames = _.pluck(filteredImages, "name");
              expect(resultNames).to.contain("eastwood.png");
              expect(resultNames).to.contain("jayz.png");
              expect(resultNames).to.contain("hopper.png");

              callback();
            }
          });

        },
        function anotherSearchWithOR(callback) {

          var filterByTag = {
            "groupOp":"OR",
            "rules":[
              {
                "field":"tags",
                "op":"eq",
                "data":"america"
              },
              {
                "field":"tags",
                "op":"eq",
                "data":"trips"
              }
            ]
          };

          var filteredImages = null;

          imageService.findByTags(filterByTag, null, function (err, images) {
            if (err) {
              done(err);
            } else {
              filteredImages = images;
              expect(filteredImages).to.have.length(2);
              var resultNames = _.pluck(filteredImages, "name");
              expect(resultNames).to.contain("eastwood.png");
              expect(resultNames).to.contain("hopper.png");

              callback();
            }
          });

        }


      ], function (err, results) {
        done();
      });


    });//end it

    after(function (done) {
      dbMan.destroyDatabase(options, done);
    });//end after


  });//end describe

  describe('testing ImageService replace tags', function () {

    /*
     - pick 3 images path from 'test/resources/images'
     - create an image record in couch using ImageService.save(imagePath)
     - add and save tags for them
     - test1: that the retrieved images have the tags in alphabetical order
     - test the tagsReplace method
     *
     * */

    var path_to_images = './test/resources/images';
    var theSavedImages = {};
    var theRetrievedImages = {};

    var theOriginalTagsMap = {};
    theOriginalTagsMap["eastwood.png"] = ["trips", "family", "friends"];
    theOriginalTagsMap["hopper.png"] = ["zoo", "america", "friends"];
    theOriginalTagsMap["jayz.png"] = ["f", "l", "family", "friends"];

    var theExpectedOrderedTagsMap = {};
    theExpectedOrderedTagsMap["eastwood.png"] = ["family", "friends", "trips"];
    theExpectedOrderedTagsMap["hopper.png"] = ["america", "friends", "zoo"];
    theExpectedOrderedTagsMap["jayz.png"] = ["f", "family", "friends", "l"];


    before(function (done) {


      //create test database
      dbMan.startDatabase(options);

      var imagesPaths = [path_to_images + '/eastwood.png',
        path_to_images + '/hopper.png',
        path_to_images + '/jayz.png'];


      function ingest(anImagePath, next) {
        var saveOptions={retrieveSavedImage:true};
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

        function saveImagesWithAttachments(callback) {
          async.forEach(imagesPaths, ingest, function (err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });
        },

        function updateImagesWithTheTags(callback) {

          _.forEach(_.keys(theSavedImages), function (key) {
            theSavedImages[key].tagsAdd(theOriginalTagsMap[key]);
          });


          function updateImage(image, next) {
            imageService.saveOrUpdate(
              {"doc":image, "tried":0},
              function (err, result) {
                if (err) {
                  console.log(err);
                  done(err);
                }

                imageService.show(result.id, null, function (err, image) {
                  if (err) {
                    done(err);
                  } else {
                    theRetrievedImages[image.name] = image;
                    next();
                  }
                });


              }
            );

          }

          async.forEach(_.values(theSavedImages), updateImage, function (err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });

        },

        function replaceSomeTags(callback) {

          //var eastwoodImage = theSavedImages["eastwood.png"];
          var oidsOfRetrievedImages = _.pluck(theRetrievedImages, "oid");

          var oldTags = ["family","friends"]
            , newTags = ["family and pets","fun"];

          imageService.tagsReplace(oidsOfRetrievedImages, oldTags, newTags,
            function (err) {
              if (err) {
                console.log(err);
                done(err);
              }
              callback(null);
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
    it("The replaced images should have the given tags replaced", function (done) {


        //Find Images with replaced tags
        var imagesWithReplacedTags = null;
        var oidsOfRetrievedImages = _.pluck(theRetrievedImages, "oid");

        async.waterfall(
          [
            //Retrieve the modified images
            function (next) {

              imageService.findByOids(oidsOfRetrievedImages, null, function (err, images) {
                if (err) {
                  done(err);
                }
                else {
                  imagesWithReplacedTags = images;
                  next();
                }
              });

            },
            function testReplacedTags(next) {

              _.forEach(imagesWithReplacedTags, function (imageWithReplacedTags) {
                switch (imageWithReplacedTags.name) {
                  case "eastwood.png":
                    expect(imageWithReplacedTags.tagsGet()).to.deep.equal(["family and pets", "fun", "trips"]);
                    break;
                  case "hopper.png":
                    expect(imageWithReplacedTags.tagsGet()).to.deep.equal(["america", "fun", "zoo"]);
                    break;
                  case "jayz.png":
                    expect(imageWithReplacedTags.tagsGet()).to.deep.equal(["f", "family and pets", "fun", "l"]);
                    break;
                  default :
                }

              });
              next();


            }

          ],

          // called after waterfall completes
          function (err) {
            if (err) {
              callback(err);
            } else {
              done();
            }
          }
        );

      }
    );//end it

    after(function (done) {
      dbMan.destroyDatabase(options, done);
    });//end after

  });//end describe


  describe('testing ImageService get list of distinct tags', function () {

    /*
     - pick 3 images path from 'test/resources/images'
     - create an image record in couch using ImageService.save(imagePath)
     - add and save tags for them
     - test1: get the list of distinct tags
     *
     * */

    var path_to_images = './test/resources/images';
    var theSavedImages = {};
    var theRetrievedImages = {};

    var theOriginalTagsMap = {};
    theOriginalTagsMap["eastwood.png"] = ["trips", "family", "friends"];
    theOriginalTagsMap["hopper.png"] = ["zoo", "america", "friends"];
    theOriginalTagsMap["jayz.png"] = ["family", "friends"];

    var theExpectedListOfDistinctTags = ["america","family", "friends", "trips", "zoo"];


    before(function (done) {


      //create test database
      dbMan.startDatabase(options);

      var imagesPaths = [path_to_images + '/eastwood.png',
        path_to_images + '/hopper.png',
        path_to_images + '/jayz.png'];


      function ingest(anImagePath, next) {
        var saveOptions={retrieveSavedImage:true};
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

        function saveImagesWithAttachments(callback) {
          async.forEach(imagesPaths, ingest, function (err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });
        },

        function updateImagesWithTheTags(callback) {

          _.forEach(_.keys(theSavedImages), function (key) {
            theSavedImages[key].tagsAdd(theOriginalTagsMap[key]);
          });


          function updateImage(image, next) {
            imageService.saveOrUpdate(
              {"doc":image, "tried":0},
              function (err, result) {
                if (err) {
                  console.log(err);
                  done(err);
                }

                imageService.show(result.id, null, function (err, image) {
                  if (err) {
                    done(err);
                  } else {
                    theRetrievedImages[image.name] = image;
                    next();
                  }
                });


              }
            );

          }

          async.forEach(_.values(theSavedImages), updateImage, function (err) {
            if (err) {
              console.log("failed with error %j", err);
              done(err);
            }
            console.log("done!");
            callback();
          });

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
    it("The list of all tags in database should not contain duplicates", function (done) {

        var listOfAllTagsInDatabase = null;

        async.waterfall(
          [
            //Retrieve the modified images
            function (next) {

              imageService.tagsGetAll(function (err, tags) {
                if (err) {
                  done(err);
                }
                else {
                  listOfAllTagsInDatabase = tags;
                  next();
                }
              });

            },
            function testTagsGetAll(next) {
              expect(listOfAllTagsInDatabase).to.deep.equal(theExpectedListOfDistinctTags);
              next();
            }

          ],

          // called after waterfall completes
          function (err) {
            if (err) {
              callback(err);
            } else {
              done();
            }
          }
        );

      }
    );

    after(function (done) {
      dbMan.destroyDatabase(options, done);
    });//end after

  });//end describe

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {
    /*dbMan.destroyDatabase(function () {
      done();
    });*/

    done();
  });//end after

});
