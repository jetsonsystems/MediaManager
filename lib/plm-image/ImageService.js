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
  console.log('plm-image/ImageService: Checking config - ' + JSON.stringify(config) + '...');
  if (!config.db.name) {
    throw "plm-image/ImageService: ImageService.config.db.name must contain a valid database name!";
  }
  console.log('plm-image/ImageService: Config OK...');
};

exports.save = function(imagePath, callback) 
{
  checkConfig();
  var db = new(cradle.Connection)('http://' + config.db.host,
                                  config.db.port).database(config.db.name),
      image = new Image({path:imagePath});

  // console.log("new image: " + JSON.stringify(image,null,"  "));

  Step(
    function () {
      console.log("calculating checksum...");
      cs.gen(fs.createReadStream(imagePath), this);
    },

    function (ss) { 
      console.log("done with checksum: " + ss);
      image.checksum = ss;
      // console.log("checksumed image: " + JSON.stringify(image,null,"  "));
      this();
    },
    
    function () {
      console.log("parsing image file...");
      gm(fs.createReadStream(imagePath)).identify(this);
    },

    function (err, data) {
      console.log("populating image");
      if (err) { callback(err); return; }
      image.readFromGraphicsMagick(data);
      console.log("parsed image: " + JSON.stringify(image,null,"  "));
      this(err, image);
    },

    function (err, anImage) {
      console.log("saving to db...");
      if (err) { callback(err); return; }
      db.save(anImage.path, anImage, this);
    },

    function (err, result) {
      console.log("saving attachment...");
      if (err) { callback(err); return; }
      console.log("result from 'save': " + JSON.stringify(result));
      
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
