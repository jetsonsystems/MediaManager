var fs = require('fs');
var crypto = require('crypto');

var img_stream = fs.ReadStream('try_crypto.js');
var checksum = crypto.createHash('md5');

img_stream.on('data', function(bits) { checksum.update(bits); });
img_stream.on('end', function() { console.log('checksum: ' + checksum.digest('hex')); });
