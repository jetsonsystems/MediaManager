'use strict';
var crypto = require('crypto');

exports.gen = function gen(stream, callback) {
  var hash = crypto.createHash('md5');
  stream.on('data', function(bits) { hash.update(bits);});
  stream.on('end', function() { callback( hash.digest('hex') );});
};
