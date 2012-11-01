'use strict';
var hash = require('crypto').createHash('md5');

exports.gen = function gen(stream, callback) {
  stream.on('data', function(bits) { hash.update(bits);});
  stream.on('end', function() { callback( hash.digest('hex') );});
};
