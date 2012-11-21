'use strict';

var fs = require('fs')
   ,gm = require('gm')
   ,str = require('underscore.string')
   ,mock = require('mockdata')
;

var 
  source_dir = './test/resources'
  ,target_dir = source_dir +'/'+ 'gen1'
  ,source = [
    "clooney",
    "eastwood",
    "hopper",
    "jayz",
    "murray",
    "nicholson",
    "obama",
    "rourke",
    "timberlake",
    "walken",
    "zach"
  ]
;

if (!fs.existsSync(target_dir)) { fs.mkdirSync(target_dir);}

var limit = source.length;
// var limit = 4;

for (var i = 0; i < limit ; i++) {
  for (var j = i+1 ; j < limit ; j++) {

    // create a somewhat random name for the new image
    var name = source[i].substring(0,4) + ("aeiou")[mock.n(5)] + source[j].substring(source[j].length-4,source[j].length);
    // console.log(name);
    
    var target = target_dir + '/' + name + '.png';

    // create closure so that we can properly log the image being processed
    (function() {
      var current = name;
      console.log("creating %j ...", current);
      gm(source_dir + '/' + source[i] + '.png') // img1 to be morphed
        .morph(
          source_dir + '/' + source[j] + '.png', // img2 to be morphed
          target,
          function() { console.log("created %j", current);}
        ); 
    })();
  }
}


