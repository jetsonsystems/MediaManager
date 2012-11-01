'use strict';
var  fs  = require('fs')
    ,gm  = require('gm')
    ,cs  = require('./checksum')
    ,Image  = require('./Image')
    ,cradle = require('cradle')
    ,Step   = require('step')
;


exports.save = function(imagePath, callback) 
{
  // var image;
  var 
     stream = fs.createReadStream(imagePath)
    ,checksum = ''
    ,db = new(cradle.Connection)().database('plm_staging')
    ,image = {}
  ;

  Step(
    function genChecksum() {
      console.log("calculating checksum...");
      cs.gen(stream, function (s) { checksum = s; } );
      this();
    },
    
    function readFile() {
      console.log("calling readFile...");
      gm(stream).identify(this);
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
