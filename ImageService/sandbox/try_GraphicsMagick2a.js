var fs = require('fs')
   ,gm = require('gm')
;

// var img_name = 'clooawood.png';
var img_name = 'clooawood'
    ,readStream = fs.createReadStream(img_name+'.png')
;

var  name1 = img_name+'_sm1.png'
    ,name2 = img_name+'_sm2.png'
    ,name3 = img_name+'_sm3.jpg'
;

var file1 = fs.createWriteStream(name1);
var file2 = fs.createWriteStream(name2);
var file3 = fs.createWriteStream(name3);

/*
 * The following shows that when using 'bufferStream:true', the image may be streamed multiple times
 * Note also that multiple 'resize' applied in succession do not apply on top of each other.  In
 * other words, calling .resize(width/2...).resize(width/2...) is *not* the same as .resize(width/4...)
 */

gm(readStream, img_name).identify({bufferStream:true},function(err, data) {
  if (err) console.log(err);
  // console.log(data);
  this.resize(data.size.width/2, data.size.height/2)
    .resize(data.size.width/2, data.size.height/2);
  
  this.stream(function (err,stdout,stderr) { 
    console.log('resize once');
    stdout.pipe(file1); 
  });
 
  var that = this;

  // outputting a second time after a delay, checking that the image has not gone away
  // note that by the time that this executes, the 'this.resize(../8)' below will have taken effect,
  // and the 'stream' will be in 'jpg' format as a result of the this.stream('jpg',...) further below
  setTimeout( function() {
    console.log('resize again');
    that.resize(data.size.width/4, data.size.height/4);
    that.stream(function (err,stdout,stderr) { 
    stdout.pipe(file2); 
  })}, 1000);

  this.resize(data.size.width/8, data.size.height/8)
  this.stream('jpg',function (err, stdout, stderr) {
    gm(stdout,name3).identify({bufferStream:true}, function(err, data) {
      // console.log("tiny img: %", data.size);
      console.log("tiny img: %j %j", data.size, data.format);
      this.stream(function (err,stdout,stderr) {
        stdout.pipe(file3);
      });
    });
  });
      
});
