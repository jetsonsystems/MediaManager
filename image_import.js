'use strict'
var 
  _   = require('underscore')
  ,fs = require('fs')
  ,async = require('async')
  ,is = require('./lib/plm-image/ImageService')
;

var path_to_import = './test/resources/gen1';

var options = { 
  saveOriginal: true
  ,desiredVariants: [
   { name: "thumb.jpg", height: 150, format: 'JPG'}
  ,{ name: "screen.jpg", height: 450, format: 'JPG'}
  ]
};


is.config.db.name = 'plm_staging';


var images = fs.readdirSync(path_to_import);

function ingest( anImage, next ) {
  if ( anImage.indexOf(".png") > 0) {
    console.log("importing %j", anImage);
    is.save(path_to_import + "/" + anImage, next, options);
  } else next();
}

async.forEachLimit( images, 1, ingest, function(err) {
  if (err) console.log("failed with error %j", err);
  console.log("done!");
});



