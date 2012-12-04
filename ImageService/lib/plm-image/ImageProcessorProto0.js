var _ = require('underscore');
var fs = require('fs');
var path = require('path');
var im = require('imagemagick');
var Step = require('step');

var ConsoleLogger = function(debugLevel) {

  this.debugLevel = debugLevel;
  this.module = '/ImageService/lib/plm-image/ImageProcessorProto0';

  this.log = function(context, msg) {
    debugLevel <= 0 || console.log(this.module + '.' + context + ': ' + msg);
  }
};

var cLogger = new ConsoleLogger(1);

var config = {
  workDir: '/var/tmp/'
};

exports.config = config;

//
//  processImage(imagePath, callback[, options])
//
//    parameters:
//
//      * imagePath: path to an image file.
//      * desiredVariants: <desired variant spec>
//
//          <desired variant spec> ::= { name: <variant name>, format: <format spec>[, width: <maximum width>][, height: <maximum height>] }
//          <format spec> ::= "JPG" | "PNG"
//
//      * callback(err, result): Callback on success or failure.
//        * result: array of result objects.
//          Each result object contains:
//          * name, format, width, height
//          * path: Path to processed image.
//
//      * options: N/A
//
exports.processImage = function(imagePath, desiredVariants, callback, options) {
  cLogger.log('processImage', 'Processing image - ' + imagePath);
  var doVariant = function(variant, onDone) {
    var basename = _.last(imagePath.split('/'));
    var re = /\.[^\.]+$/;
    basename = basename.replace(re, '');
    var workPath = path.join(config.workDir, basename) + '-' + variant.width + 'x' + variant.height + '.jpg';
    console.log('process-some: ' + imagePath + ' -> ' + workPath);
    var options = {srcPath: imagePath,
                   dstPath: workPath,
                   width: variant.width,
                   height: variant.height};
    im.resize(options,
              function(err) {

                if (err) {
                  cLogger.log('processImage', 'error processing - ' + imagePath + ', width - ' + variant.width + ', height - ' + variant.height + ', error - ' + err);
                  fs.unlinkSync(options.dstPath);
                }
                else {
                  cLogger.log('processImage', 'processed - ' + imagePath + ', width - ' + variant.width + ', height - ' + variant.height);
                }
                var result = {
                  name: variant.name,
                  format: variant.format,
                  width: variant.width,
                  height: variant.height,
                  path: options.dstPath
                }
                cLogger.log('processImage', 'Invoking callback w/ - ' + JSON.stringify(result));
                onDone(err, 
                       { err: err,
                         variant: result
                       });
                cLogger.log('processImage', 'Invoked callback...');
              });
  };
  Step(
    function doAll() {
      var that = this;
      _.each(desiredVariants,
               function(variant) {
                 doVariant(variant, that.parallel());
               });
    },
    function finalize(err) {
      cLogger.log('processImage', 'All variants generated...');
      var results = [];
      _.each(_.rest(arguments),
             function(result) {
               cLogger.log('processImage', 'Add variant');
               if (result.err) {
                 console.log('process-some: error generating variant - ' + result.variant.name);
               }
               else {
                 results.push(result.variant);
                 console.log('process-some: successfully generated variant - ' + result.variant.name);
               }
             });
      callback(err, results);
    }
  );
}
