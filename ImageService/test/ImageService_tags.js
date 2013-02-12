var async = require("async")
;

exports.shouldPassTests = function(done){

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

      var imageService = this.imageService
      var imagesPaths = [path_to_images + '/eastwood.png',
        path_to_images + '/hopper.png',
        path_to_images + '/jayz.png'];


      function ingest(anImagePath, next) {
        imageService.save(
          anImagePath,
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

                imageService.show(result.id, function (err, image) {
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

          imageService.findByTags(filterByTag, options, function (err, images) {
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

          imageService.findByTags(filterByTag, options, function (err, images) {
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

          imageService.findByTags(filterByTag, options, function (err, images) {
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

          imageService.findByTags(filterByTag, options, function (err, images) {
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


};

