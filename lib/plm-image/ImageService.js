'use strict';
var  fs  = require('fs')
    ,gm  = require('gm')
    ,cs  = require('./checksum')
    ,Image  = require('./Image')
    ,cradle = require('cradle')
    ,Step   = require('step')
;

var config = {
  db: {
    host: "localhost",
    port: 5984,
    name: ""
  }
};

exports.config = config;

var checkConfig = function() {
  console.log('/ImageService/lib/plm-image/ImageService: Checking config - ' + JSON.stringify(config) + '...');
  if (!config.db.name) {
    throw "/ImageService/lib/plm-image/ImageService: ImageService.config.db.name must contain a valid database name!";
  }
  console.log('/ImageService/lib/plm-image/ImageService: Config OK...');
};

exports.save = function(imagePath, callback) 
{
  checkConfig();
  var checksum = '',
      db = new(cradle.Connection)('http://' + config.db.host,
                                  config.db.port).database(config.db.name),
      image = {};

  Step(
    function genChecksum() {
      console.log("calculating checksum...");
      cs.gen(fs.createReadStream(imagePath), this);
    },

    function saveChecksum(ss) { 
      console.log("done with checksum: " + ss);
      checksum = ss;
      this();
    },
    
    function parseFile() {
      console.log("calling readFile...");
      gm(fs.createReadStream(imagePath)).identify(this);
    },

    function initImage(err, data) {
      console.log("calling initImage");
      if (err) { callback(err); return; }
      data.checksum = checksum;
      image = new Image(imagePath, data);
      this(err, image);
    },

    function save(err, anImage) {
      console.log("calling save");
      if (err) { callback(err); return; }
      console.log("parsed image: " + JSON.stringify(anImage,null,"  "));
      db.save(anImage.path, anImage, this);
    },

    function saveAttachment(err, result) {
      console.log("saving attachment...");
      if (err) { callback(err); return; }
      console.log("result from 'save': " + JSON.stringify(result));
      // stream.pipe(...)  does not work because gm(stream).identify() above closes the stream
      fs.createReadStream(imagePath).pipe(
        db.saveAttachment(
          result.id, 
          {
            name: image.path, 
            contentType: 'image/' + image.format, 
          }, callback)
      );
    }
  );
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
