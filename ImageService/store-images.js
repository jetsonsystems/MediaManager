var fs = require('fs');
var dive = require('dive');
var util = require('util');
var mime = require('mime-magic');
var Step = require('step');
var _ = require('underscore');

var imageService = require('ImageService');

var opts = require('optimist')
  .boolean('v')
  .usage('Usage: $0 [<options>] <import dir>\n\nImports images into PLM Media Manager from <import dir>.');
var argv = opts.argv;

var argsOk = function(argv) {
  if (argv._.length !== 1) {
    console.log('A single <import dir> is required.');
    return false;
  }
  return true;
}(argv);

if (!argsOk) {
  opts.showHelp();
  process.exit(1);
}

imageService.config.db.port = 59840;
imageService.config.db.name = 'plm-media-manager-test0';

var importDir = argv._[0];

var files = [];
var maxParallel = 2;

//
// saveImages: Save a set of images along with their attachments.
//
//  Args:
//    toSave: list of objects describing each image -
//      { path: <path to image>,
//        attachments: array of attachments objects -
//          { attachment: <raw bytes>
//            width: <integer pixels>
//            height: <integer pixels>
//            content-type: <mime type }
//
var saveImages = function(toSave) {
  var parallelSave = function(toSave, onSaved) {
    console.log('store-images: About to save ' + toSave.length + ' images...');
    var saveOne = function(fData, callback) {
      console.log('store-images: storing file - ' + fData.path + ' ...');
      try {
        imageService.save(
          fData.path,
          function(err, result) {
            if (err) { 
              console.log('store-images: error saving image (1) - ' + JSON.stringify(err));
            }
            else {
              console.log("store-images: saved result - " + JSON.stringify(result));
            }
            callback(err,
                     { status: err,
                       result: result });
          },
          {
            saveOriginal: false,
            desiredVariants: [{ name: 'thumbnail.jpg', format: 'jpg', width: 80, height: 80}, 
                              { name: 'web.jpg', format: 'jpg', width: 640, height: 400}, 
                              { name: 'full-small.jpg', format: 'jpg', width: 1280, height: 800}]
          }
        );
      }
      catch(err) {
        console.log('store-images: error saving image (2) - ' + err);
        callback(err,
                 { status: err,
                   result: null });
      }
    };

    Step(
      function doParallel() {
        var that = this;
        _.each(toSave, function(fData) {
          saveOne(fData, that.parallel());
        });
      },
      function finalize(err, args) {
        _.each(args,
               function(result) {
                 console.log('store-images: result - ' + JSON.stringify(result));
               });
        onSaved(err);
      }
    );
  };
  function saveMore() {
    console.log('store-images: saving more...');
    var toDo = [];
    for (i = 0; i < maxParallel && toSave.length > 0; i++) {
      toDo.push(toSave.shift());
    }
    if (toDo.length > 0) {
      parallelSave(toDo, saveMore);
    }
  }
  saveMore();
};

console.log('store-images: about to look for files in - ' + importDir);

dive(importDir, 
     {}, 
     function(err, file) {
       if (err) throw err;
       var stats = fs.statSync(file);
       if (stats.isFile()) {
         console.log('read-some: have file - ' + file);
         files.push({ path: file,
                      stats: stats
                    });
       }
       else {
         console.log('read-some: file ' + file + ' is not a file!');
       }
     },
     function() {
       mime(_.pluck(files, 'path'), function(err, types) {
         if (err) {
           console.log('read-some: Error getting file types - ' + JSON.stringify(err));
         }
         else {
           var i = 0;
           _.map(files, function(fData) {
             fData.fileType = types[i];
             console.log('read-some: file - ' + fData.path);
             console.log('read-some:      - file type = ' + fData.fileType);
             console.log('read-some:      - ' + util.format('%j', fData.stats));
             i = i + 1;
           });
           console.log('read-some: Have ' + files.length + ' files.');
           var re = /^image\//;
           saveImages(_.filter(files, function(fData){ return fData.fileType.match(re); }),
                      {}
                     );
         }
       })
     });


console.log('store-images: done');
