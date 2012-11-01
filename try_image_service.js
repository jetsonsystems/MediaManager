'use strict';
var imageService = require('./lib/plm-image/ImageService');
console.log("starting...");

imageService.save(
  "./asset/eastwood.png",
  function(err, result) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    console.log("result: " + JSON.stringify(result));
  }
);

