var fs = require('fs')
   ,gm = require('gm')
;

// var img_name = 'clooawood.png';
var img_name = 'clooawood'
    ,readStream = fs.createReadStream(img_name+'.png')
;


gm(readStream, img_name).identify(function(err, data) {
  if (err) console.log(err);
  data.name = img_name;
  console.log(data);
});
