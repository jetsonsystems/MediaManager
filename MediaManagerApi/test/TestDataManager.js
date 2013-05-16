'use strict';

var async = require('async')
  , databaseManager = require("./databaseManager")
  , _ = require('underscore');


function TestDataManager() {
  if (!(this instanceof TestDataManager)) {
    return new TestDataManager();
  }
}


/*
 * See Prototypes and Privacy
 * 2010 - JavaScript Patterns By Stoyan Stefanov
 * */
TestDataManager.prototype = (function () {
// private member

  var path_to_images = '../ImageService/test/resources/images';
  var theSavedImages = {};

  var dbOptions = {
    host:"localhost"
    ,port:5984
    ,dbName:"plm-media-manager-dev0"
    ,design_doc:'couchdb'
  };

  var imageService;

  var _setDBOptions = function (theDbOptions) {
    dbOptions = theDbOptions;
  };

  var _setImageService = function (theImageService) {
    imageService = theImageService;
  };

  var _spawnInstance = function () {
    var m = TestDataManager();
    return m;
  };

  /**
   *
   * @param options
   *        options.populateTags
   * @param callback
   * @private
   */
  var _populateTestData = function (options, callback) {


    var imagesPaths = [path_to_images + '/eastwood.png',
      path_to_images + '/hopper.png',
      path_to_images + '/jayz.png'];

    var theOriginalTagsMap = {};
    theOriginalTagsMap["eastwood.png"]  = ["trips", "family", "friends"];
    theOriginalTagsMap["hopper.png"]    = ["zoo", "america", "friends"];
    theOriginalTagsMap["jayz.png"]      = ["family", "friends"];


    function ingest(anImagePath, next) {
      var saveOptions = {
          retrieveSavedImage:true,
          desiredVariants: [
            {   name: 'thumb.jpg', format: "JPG", width: 120, height: 150}
            ,
            {  name: 'screen.jpg', format: "JPG", width: 360, height: 450}
          ]};
      imageService.save(
        anImagePath,
        saveOptions,
        function (err, result) {
          if (err) {
            console.log(err);
          }
          theSavedImages[result.name] = result;
          next();
        }
      );

    };


    async.waterfall([

      function (next) {

        //create test database
        databaseManager.startDatabase(dbOptions,
          function (err, result) {
            if (err) {
              console.log(err);
            }
            else {
              console.log("Test database created");
            }
            next(null);
          });

      },


      function saveImagesWithAttachments(next) {
        async.forEach(imagesPaths, ingest, function (err) {
          if (err) {
            console.log("failed with error %j", err);
          }
          console.log("done!");
          next();
        });
      },

      function updateImagesWithTheTags(next) {

        function updateImage(image, next) {
          imageService.saveOrUpdate(
            {"doc":image, "tried":0},
            function (err, result) {
              if (err) {
                console.log(err);
              }

              next();
            }
          );

        }

        if(options.populateTags){
          _.forEach(_.keys(theSavedImages), function (key) {
            theSavedImages[key].tagsAdd(theOriginalTagsMap[key]);
          });

          async.forEach(_.values(theSavedImages), updateImage, function (err) {
            if (err) {
              console.log("failed with error %j", err);
            }
            console.log("done!");
            next();
          });


        }else{
          next();
        }

      }
    ], function (err, results) {
      callback(null);
    });


  }

  var _getAllImages = function(callback){
    var options = null;
    imageService.index(options,callback);
  }

  var _destroyTestData = function (callback) {
    databaseManager.destroyDatabase(callback);
  };


// public exposed prototype members
  return {
    setDBOptions:_setDBOptions,
    setImageService:_setImageService,
    populateTestData:_populateTestData,
    getAllImages:_getAllImages,
    destroyTestData:_destroyTestData,
    spawnInstance:_spawnInstance
  };
}());


module.exports = TestDataManager();

