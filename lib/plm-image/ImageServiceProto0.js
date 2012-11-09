//
//  ImageServiceProto0: A prototype for the ImageService in order to expose 
//    additional CRUD methods. This is essentially throw away code.
//
//    Method Arguments:
//
//      * callback(err, result): 
//        Each method takes a 'callback' parameters as the final paramter. The 
//        signature implies two paramters will be passed back:
//          err - an error string.
//          result - a json result payload.
//
//      * options:
//        The options Argument is a JavaScript object of key/value pairs. Some 
//        common options shared accross methods include:
//
//        created: <date selector>
//          Allows for filtering of images based upon its creation date. See created_at.
//        imported: <date selector>
//          Allows for filtering of images based upon when it was imported into PLM. See imported_at.
//
//        <date selector> ::= <date> || <date range>
//
//    Dates / Time stamps:
//
//      A date value will take the form of YYYY[MM[DD[HH[mm[.SS]]]]] in UTC.
//
//    Image Objects:
//
//      Image objects represent meta-data about an image. A valid image object
//      must contain the following fields:
//
//      * id: A globally unique identifier.
//      * path: Path used to import the image.
//      * name: Name of the image. Defaults to the basename of path.
//      * created_at: Date an image was created.
//      * imported_at: Date an image was imported (or saved).
//      * variants: A list of variants of the original which are available. Each
//          variant is described with an object with the following fields:
//
//          * name: <image variant name>
//            Image variant name which will be used to refer to the variant of the image.
//            It can be assigned or automatically generated. If automatically generated,
//            the format is as follows:
//
//              <image variant name> ::= original.<format> | <derived style>
//              <derived style> ::= derived-<width>-<length>.<format>
//
//          * url: A URL to refer to the image variant.
//

'use strict';
var fs  = require('fs'),
    gm  = require('gm'),
    cs  = require('./checksum'),
    Image  = require('./Image'),
    nano = require('nano'),
    Step   = require('step'),
    _ = require('underscore'),
    ip = require('./ImageProcessorProto0.js');

var ConsoleLogger = function(debugLevel) {

  this.debugLevel = debugLevel;
  this.module = '/ImageService/lib/plm-image/ImageServiceProto0';

  this.log = function(context, msg) {
    debugLevel <= 0 || console.log(this.module + '.' + context + ': ' + msg);
  }
};

var cLogger = new ConsoleLogger(1);

var config = {
  db: {
    host: "localhost",
    port: 5984,
    name: ""
  }
};

exports.config = config;

var checkConfig = function() {
  cLogger.log('checkConfig', 'Checking config - ' + JSON.stringify(config) + '...');
  if (!config.db.name) {
    throw "/ImageService/lib/plm-image/ImageService: ImageService.config.db.name must contain a valid database name!";
  }
  cLogger.log('checkConfig', 'Config OK...');
};

var dbServer = null;
var getDbServer = function() {
  cLogger.log('getDbServer', 'Connecting to db host - ' + config.db.host + ', port - ' + config.db.port);
  dbServer = dbServer || nano('http://' + config.db.host + ":" + config.db.port);
  cLogger.log('getDbServer', 'Connected to db host - ' + config.db.host + ', port - ' + config.db.port);
  return dbServer;
};

var getDb = function() {
  return getDbServer().use(config.db.name);
};

//
//  save(imagePath, callback, [options])
//
//    Extracts meta-data from one or more image files, saves the image meta-data.
//    Optionally, the image itself is stored, and the image processed to created
//    derived variants according to requested specifications.
//
//    parameters:
//
//      * options: A hash of options, which may include:
//        * saveOriginal: Whether to save the original, true or false. Default is false.
//        * desiredVariants: An array of specifications for the creation of derived image variants.
//
//            <desired variant spec> ::= { name: <variant name>, format: <format spec>[, width: <maximum width>][, height: <maximum height>] }
//            <format spec> ::= "jpg" | "png"
//
//
//    example:
//
//      var imageService = require('ImageService');
//
//      var filePath = <path to image file>
//
//      imageService.save(filePath, 
//                        function(error, result) {}, 
//                        { saveOriginal: false,
//                          desiredVariants: [ {name: "smallThumb.jpg, format: "JPG", width: 80, height: 80}, 
//                                             {name: "webMedium.jpg", format: "JPG", width: 600, height: 600 }]})
//
exports.save = function(imagePath, callback, options) 
{
  checkConfig();
  var db = getDb();

  var sOrig = options && _.has(options, 'saveOriginal') && options.saveOriginal;
  var desiredVariants = options && _.has(options, 'desiredVariants') ? options.desiredVariants : undefined;

  var image = new Image({path:imagePath});

  Step(
    function genChecksum() {
      cLogger.log('save.genChecksum', 'Calculating checksum...');
      cs.gen(fs.createReadStream(imagePath), this);
    },

    function saveChecksum(ss) { 
      cLogger.log('save.saveChecksum', 'Done with checksum - ' + ss);
      image.checksum = ss;
      this();
    },
    
    function readFile() {
      cLogger.log('save.readFile', 'calling readFile...');
      gm(fs.createReadStream(imagePath)).identify(this);
      cLogger.log('save.readFile', 'finished readFile...');
    },

    function loadImage(err, data) {
      cLogger.log('save.loadImage', 'starting...');
      if (err) {
        cLogger.log('save.loadImage', 'error - ' + err);
        callback(err, null); return; 
      }
      image.readFromGraphicsMagick(data);
      cLogger.log('save.loadImage', 'Created image, path - ' + image.path);
      this(err, image);
    },

    function save(err, anImage) {
      cLogger.log('save.save', 'About to save meta-data...');
      if (err) { callback(err); return; }
      // cLogger.log('save.save', 'parsed image - ' + JSON.stringify(anImage,null,"  "));
      db.insert(anImage, anImage.path, this);
    },

    function saveOriginal(err, result) {
      var that = this;
      if (err) { 
        cLogger.log('save.saveOriginal', 'err - ' + err);
        callback(err, result); return; 
      }
      else {
        cLogger.log('save.saveOriginal', 'saved meta data for - ' + JSON.stringify(result));
      }

      // cLogger.log('save.saveOriginal', "result from 'save': " + JSON.stringify(result));
      if (sOrig) {
        var docRev = result._rev;
        var attachmentName = _.last(imagePath.split('/'));
        cLogger.log('save.saveOriginal', 'saving original, doc - ' + imagePath + ', attachment name - ' + attachmentName);
        fs.createReadStream(imagePath).pipe(
          db.attachment.insert(imagePath,
                               attachmentName,
                               null,
                               'image/' + image.format,
                               {rev: docRev},
                               that)
        );
      }
      else {
        that(null, result);
      }
    },

    //
    //  doDesiredVariants: Generate and save variants which are desired.
    //
    function doDesiredVariants(err, result) {
      var that = this;
      if (desiredVariants) {
        ip.processImage(imagePath, 
                        desiredVariants,
                        that);
      }
      else {
        callback(err, result);
      }
    },

    //
    //  saveDesiredVariants:
    //
    function saveDesiredVariants(err, result) {
      if (err) {
        cLogger.log('save.saveDesiredVariants', 'Error processing image - ' + imagePath);
        callback(err, result);
      }
      cLogger.log('saveDesiredVariants', JSON.stringify(result));
      var todo = _.clone(result);
      var numToDo = todo.length;
      var done = [];
      var error = undefined;

      var saveVariants = function() {
        var saveVariant = function() {
          if ((error === undefined) && (todo.length > 0)) {
            var variant = todo.shift();
            
            db.get(imagePath, {}, function(err, doc) {
              cLogger.log('saveDesiredVariants', 'Getting doc rev for doc - ' + imagePath);
              if (err) {
                cLogger.log('saveDesiredVariants', 'Error getting doc rev for doc - ' + imagePath + ', error - ' + err);
                error = err;
                callback(err, null);
              }
              else {
                cLogger.log('saveDesiredVariants', 'Saving variant - ' + variant.path + ', doc rev - ' + doc._rev);
                fs.createReadStream(variant.path).pipe(
                  db.attachment.insert(imagePath,
                                       variant.name,
                                       null,
                                       'image/' + image.format,
                                       {rev: doc._rev},
                                       function(err, result) {
                                         if (err) {
                                           callback(err, result);
                                         }
                                         else {
                                           done.push(variant);
                                           if (done.length === numToDo) {
                                             callback(err, result);
                                           }
                                           else {
                                             saveVariant();
                                           }
                                         }
                                       }));
              }
            });
          }
        };
        saveVariant();
      }
      saveVariants();
    }
  );
};

//
//  index: Returns a list of images.
//
//    callback(err, result):
//      err - error string.
//      result - a JSON list of images.
//
exports.index = function(callback) {
  checkConfig();
  cLogger.log('index', 'Connecting to db...');
  var db;
  try {
    db = getDb();
    cLogger.log('index', 'Got db...');
  }
  catch (error) {
    cLogger.log('index', 'DB connection failed w/ error - ' + error);
    throw error;
  }
  cLogger.log('index', 'Connected to db...');
  db.list(function(err, body) {
    if (err) {
      callback(err, body);
    }
    else {
      //
      // Request each document in the DB.
      //
      var error = undefined;
      var result = [];
      var docIds = _.pluck(body.rows, 'id');
      var numToDo = docIds.length;

      var doDocs = function(callback) {
        var doDoc = function(callback) {
          if ((error === undefined) && (docIds.length > 0)) {
            var docId = docIds.shift();

            cLogger.log('index', 'Fetching document w/ id - ' + docId);

            db.get(docId, 
                   {},
                   function(err, docBody) {
                     if (err) {
                       cLogger.log('index', 'Received error getting document w/ id - ' + docId + ', error - ' + err);
                       error = err;
                       callback(error, result);
                     }
                     else {
                       cLogger.log('index', 'Appending doc. body to result set...');
                       result.push(docBody);
                       if (result.length === numToDo) {
                         cLogger.log('index', 'Finished processing documents, invoking callback...');
                         callback(error, result);
                       }
                       else {
                         cLogger.log('index', 'Doing more, result set size - ' + result.length + ', initial workload - ' + numToDo);
                         doDoc(callback);
                       }
                     }
                   });
          }
        }
        doDoc(callback);
      }
      doDocs(callback);
      cLogger.log('index', 'Initated getting documents!');
    }
  });
};

/*
console.log("starting...");

parseAndSaveImage(
  // "images/common/icons/snow_18px_trans.png", 
  "/home/philippe/multimedia/pictures/2012/2011-10-31_10-11-14.jpg",
  function(err, result) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    console.log("result: " + JSON.stringify(result));
  }
);
*/
