#! /usr/bin/node

'use strict';
var 
  imageService = require('./lib/plm-image/ImageService')
  ,_ = require('underscore')
  ,async = require('async')
  ,fs = require('fs')
  ,log4js = require('log4js')
  ,str  = require('underscore.string')
  ,util = require('util')
;


var root_dir = './test/resources';

imageService.config.db.name = 'plm_staging';

console.log("starting...");

var asset = ['eastwood','obama','clooney','jayz'];

log4js.configure({
  appenders: [
    {
      type: "file"
      ,filename: "service.log"
      ,category: [ 'plm.ImageService' ]
    },
    /*
    {
      type: "console"
    }
    */
  ]
  ,replaceConsole: false
  ,levels: { 'plm.ImageService' : 'DEBUG' }
});

/*
imageService.parseImage(
  root_dir + "/2011-10-31_10-11-54.jpg",
  function(err, imgData, imgStream) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    console.log("imgData: " + JSON.stringify(imgData,null,'  '));
  }
);
*/

/*
imageService.parseAndTransform(
  root_dir + "/eastwood.png",
  { saveOriginal: true 
    ,desiredVariants: [ 
      { name: 'eastwood_thumb.jpg', format: "JPG", width: 120, height: 150} 
    ]
  },
  function(err, result) {
    if (err) { console.log(err); }

    var i = 0;

    function iterator(element, next) {
      console.log("inspect: " + util.inspect(result));
      var name = 'out-'+i+'.'+element.data.format.toLowerCase();
      var file = fs.createWriteStream(name);
      console.log("outputting file: %j",name);
      fs.createReadStream(element.stream).pipe(file);
      // element.stream.pipe(file);
      i++
      next();
    }

    async.forEachSeries(result, iterator, function(err) { console.log('done');});
  }
);
*/

/*
var target_dir = 'test/resources/empty';
// var target_dir = '/home/philippe/multimedia';
imageService.collectImagesInDir(target_dir, function(err, images) {
  if (err) { console.log('err: %s', err)}
  console.log("done!");
  // _.each(images, function(f) { console.log('%j', f);});
});
*/

// for operations that use couch, try them inside the callback to checkConfig
imageService.checkConfig( function(err, result) {

if (err) {
  console.log(err); 
  return;
} else {
  console.log("db is ok:%j", result);
  // console.log("db is ok:\n%s", util.inspect(result));

// simple save
/*
imageService.save(
  root_dir + "/eastwood.png",
  null,
  function(err, result) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    console.log("result: " + JSON.stringify(result));
    // console.log("inspect: " + util.inspect(result));
  }
);
*/


// save with variants
/*
imageService.save(
  root_dir + "/eastwood.png",
  null,
  function(err, result) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    console.log("result: %j", result);
    // console.log("inspect: %s", util.inspect(result));
  },
  { saveOriginal: true 
    ,desiredVariants: [ 
       { name: 'thumb.jpg',  format: "JPG", width: 120, height: 150} 
      ,{ name: 'screen.jpg', format: "JPG", width: 360, height: 450} 
    ]
  }
);
*/


// save all inside the array asset
/*
_.each(asset, 
  function(name) {
    var img_path = root_dir + '/' + name + '.png';
    console.log("saving %j", img_path);
    
    imageService.save(
      img_path,
      null,
      function(err, result) {
        if (err) { console.log(err); }
        console.log("result: " + JSON.stringify(result));
      }
    );
    
  });
*/


/*
imageService.findVersion(root_dir + '/eastwood.png', function (version) {
  console.log("img version: %j", version);
});
*/


// save with variants
/*
imageService.save(
  root_dir + "/eastwood.png",
  null,
  function(err, result) {
    // if (err) { console.log(err); throw err; }
    if (err) { console.log(err); }
    // console.log("result: " + JSON.stringify(result));
    console.log("inspect: " + util.inspect(result));
  },
  null,
  { saveOriginal: true 
    ,desiredVariants: [ 
      { name: 'eastwood_thumb.jpg', format: "JPG", width: 120, height: 150} 
    ]
  }
);
*/


// show by oid
/*
var oid = '0830a27b-fa78-4f7d-8f92-f865822e9e95';
// var oid ='somebogus-oid'
imageService.show(oid, null, function(err, image) {
  if (err) { console.log("error: " + err); return; };
  //console.log('retrieved image with oid %j: ' +  JSON.stringify(image,null,'  '), oid);
  console.log("retrieved image with oid '%s': %s", oid, util.inspect(image, true, null));
});
*/


// return all by default
/*
imageService.index(
  function(err, aryImage) {
    if (err) { console.log(err); return; }

    // console.log("result: "  + util.inspect(aryImage));

    _.each(aryImage, function(image) {
      console.log('retrieved image with oid %j: %j - created_at: %j ', image.oid, image.path, image.created_at);
    });

  }
);
*/


// find by date range
/*
imageService.index(
  function(err, aryImage) {
    if (err) { console.log(err); return; }

    // console.log("result: "  + util.inspect(aryImage));

    _.each(aryImage, function(image) {
      console.log('retrieved image with oid %j: %j - created_at: %j ', image.oid, image.path, image.created_at);
    });

  },
  { created: ["20121101", "20121201"] }
);
*/


// batch import from fs
var target_dir = "/home/philippe/project/jetsonsys/src/ImageService/test/resources/gen2/eastwood";
// var target_dir = "test/resources/empty";
var options = {};
imageService.importBatchFs(
  target_dir, 
  function(err, importBatch) {
    if (err) console.log("err: %s", err);
    else { 
      console.log("importBatch: %s", util.inspect(importBatch,null,'  '));

      var oid = importBatch.oid;
      var checkBatch = setInterval( function() {
        imageService.importBatchShow(oid, {}, function(err, importBatch) {
          if (err) console.log("err: %s", err);
          console.log("checking status of importBatch '%s': %j", oid, importBatch);
          if (importBatch.status === 'COMPLETED') {
            console.log("importBatch completed!");
            clearInterval(checkBatch);
          }
        });
      }, 500);
    }
  }, 
  { saveOriginal: true
    ,retrieveSavedImage:true
    ,desiredVariants: [ 
       { name: 'thumb.jpg',  format: "JPG", width: 120, height: 150} 
      ,{ name: 'screen.jpg', format: "JPG", width: 360, height: 450} 
    ]
  }
);

// importBatchFindRecent without images
/*
imageService.importBatchFindRecent(8, null, function(err, batches) {
  if (err) console.log(err);
  else console.log("done retrieving %s batches: %j", batches.length, batches);
});
*/

// importBatchShow by oid
/*
var oid = '07e5be5d-b391-458a-8c79-580a19fc1d63';
//imageService.importBatchShow(oid, {includeImages: false}, function(err, out) {
imageService.importBatchShow(oid, null, function(err, out) {
  if (err) console.log(err);
  else console.log("importBatch: %j", out);
  // else console.log("importBatch: %s", util.inspect(out,false,2));
});
*/

// importBatchFindRecent with images
/*
imageService.importBatchFindRecent(5, {includeImages: true}, function(err, batches) {
  if (err) console.log(err);
  else console.log("done retrieving %s batches: %j", batches.length, batches);
  // else console.log("done retrieving %s batches:\n%s", batches.length, util.inspect(batches,false,null));
});
*/



  } // else from way above
}); // the initial checkConfig call
