'use strict';
var cs = require('../checksum');
var fs = require('fs');

// the following two lines do the same, the 'create' can be passed options
// var img_stream = fs.ReadStream('image_test/cloonolson.png');
var img_stream = fs.createReadStream('clooawood.png');

cs.gen(img_stream, 
  function(s){ 
    console.log(s);
  }
);

// cs.gen(img_stream);

