var fs = require('fs')
   ,gm = require('gm')
;

var img_name = 'clooawood.png';

gm(stream, img_name).identify(function(err, data) {
  if (err) console.log(err);
  data.name = img_name;
  console.log(data);
});
