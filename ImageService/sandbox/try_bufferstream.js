'use strict';

var 
   fs  = require('fs')
  ,BufferStream = require('bufferstream')
;

var filestream = fs.createReadStream('try_image.js');
var stream = new BufferStream();
var aryStream = [];

stream.setSize('flexible');

filestream.on('data', function(bits) { 
  // console.log('writing bits: %j',bits);
  stream.write(bits);
  aryStream.push(bits);

});

stream.on('data', function(bits) {
  // console.log('reading bits: %j',bits);
});

/*
stream.on('end', function() { 
  console.log('stream1 is: %j',stream.toString()); 
});
*/

filestream.on('end', function() { 
  console.log('stream2 is: %j',stream.toString()); 
});

filestream.pipe(process.stdout);

stream.resume();
stream.pipe(process.stdout);




