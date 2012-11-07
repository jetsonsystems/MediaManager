'use strict';
var 
  imageService = require('./lib/plm-image/ImageService')
  ,_ = require('underscore');
;
imageService.config.db.name = 'plm_staging';

console.log("starting...");

var asset = ['eastwood','obama','clooney','jayz'];


_.each(asset, 
  function(name) {
    var img_path = './asset/' + name + '.png';
    console.log("saving %j", img_path);
    
    imageService.save(
      img_path,
      function(err, result) {
        if (err) { console.log(err); }
        console.log("result: " + JSON.stringify(result));
      }
    );
    
  });


/*
imageService.save(
  "./asset/eastwood.png",
  function(err, result) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    console.log("result: " + JSON.stringify(result));
  }
);
*/

