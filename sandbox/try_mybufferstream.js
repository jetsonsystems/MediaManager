'use strict';

var 
   fs  = require('fs')
;

//var filestream = fs.createReadStream('try_image.js');
var filestream = fs.createReadStream('clooawood.png');
var aryStream = [];

var filewrite = fs.createWriteStream('clooawood_copy.png');

filestream.on('data', function(bits) { 
  // console.log('writing bits: %j',bits);
  aryStream.push(bits);

});

filestream.on('end', function() { 
  for (var i = 0; i < aryStream.length; i++) {
    console.log("buf bits %j: %j", i, Buffer.isBuffer(aryStream[i]));
    filewrite.write(aryStream[i]);
  }
});

/*
console.log('pipe1');
filestream.pipe(process.stdout);
*/
