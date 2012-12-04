var 
  fs = require('fs'), 
  gm = require('gm'),
  Image = require('../lib/plm-image/Image');
  ;

var img_path = 'asset/eastwood.png';

gm(img_path).identify(function(err, data) {
		if (err) console.log(err);
    img = new Image(img_path);
    img.gm(data);
		console.log(JSON.stringify(img,null,'  '));
});

