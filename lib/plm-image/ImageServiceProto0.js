//
//  ImageServiceProto0: A prototype for the ImageService in order to expose 
//    additional CRUD methods. This is essentially throw away code.
//
//    General Notes:
//
//      * callback(err, result): Each method takes a 'callback' parameters as 
//        the final paramter. The signature implies two paramters will be passed
//        back:
//          err - an error string.
//          result - a json result payload.
//

'use strict';
var fs  = require('fs'),
    gm  = require('gm'),
    cs  = require('./checksum'),
    Image  = require('./Image'),
    nano = require('nano'),
    Step   = require('step'),
    _ = require('underscore');

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



exports.save = function(imagePath, callback) 
{
  checkConfig();
  var stream = fs.createReadStream(imagePath),
      checksum = '',
      db = getDb(),
      image = {};

  Step(
    function genChecksum() {
      console.log(dbgPrefix + '.genCheckSum: Calculating checksum...');
      var group = this.group();
      cs.gen(stream, group());
      console.log(dbgPrefix + '.genCheckSum: Done with checksum...');
      this();
    },

    function saveChecksum(err, ss) { 
      _.first(ss, function(s) {
        checksum = s;
      });
    },
    
    function readFile() {
      console.log(dbgPrefix + ".readFile: calling readFile...");
      gm(stream).identify(this);
      console.log(dbgPrefix + ".readFile: finished readFile...");
    },

    function initImage(err, data) {
      console.log(dbgPrefix + ".initImage: starting...");
      if (err) {
        console.log(dbgPrefix + ".initImage: error - " + err);
        callback(err, null); return; 
      }
      var image = null;
      // console.log(dbgPrefix + '.initImage: have image data - ' + JSON.stringify(data));
      if (data) {
        data.checksum = checksum;
        image = new Image(imagePath, data);
        console.log(dbgPrefix + '.initImage: Created image, path - ' + image.path);
      }
      this(err, image);
    },

    function save(err, anImage) {
      console.log(dbgPrefix + '.save: About to save...');
      if (err) { callback(err); return; }
      // console.log("parsed image: " + JSON.stringify(anImage,null,"  "));
      db.insert(anImage, anImage.path, this);
    },

    function saveAttachment(err, result) {
      if (err) { 
        console.log(dbgPrefix + ".saveAttachment: err - " + err);
        callback(err); return; 
      }
      console.log(dbgPrefix + ".saveAttachment: result from 'save': " + JSON.stringify(result));
      var docRev = result.rev;
      var attachmentName = _.last(imagePath.split('/'));
      console.log(dbgPrefix + '.saveAttachment: saving attachment, doc - ' + imagePath + ', attachment name - ' + attachmentName);
      fs.createReadStream(imagePath).pipe(
        db.attachment.insert(imagePath,
                             attachmentName,
                             null,
                             'image/' + image.format,
                             {rev: docRev},
                             callback)
      );
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
