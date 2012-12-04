var fs = require('fs')
   ,gm = require('gm')
;

var img_name = 'clooawood';
    ,readStream = fs.createReadStream(img_name+'.png')
;

gm(readStream, img_name)
  .identify({bufferStream: true}, function(err, data) {
    // if (!err) console.log(data);
    this.resize(data.size.width/2,data.size.height/2);
    this.stream( function(err, stdout, stderr) {
      if (err) console.log('unable to read resized image',err);  
      gm(stdout, img_name+'_small.jpg').identify( {bufferStream: true}, function(err, data) 
      {
        console.log('resized image: %j', data);
        this.stream( function(err, stdout, stderr) {
          var writeStream = fs.createWriteStream(img_name+'_small.jpg');
          stdout.pipe(writeStream);
        });
      });
    });
  });
