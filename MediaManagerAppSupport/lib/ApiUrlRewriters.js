//
// ApiUrlRewriters: Transform URLs in API resources.
//
//  Public API:
//
//    images: rewrite image URLs.
//    importerImages: rewrite image URLs in an /importers/images resource.
//

var path = require('path');
var log4js = require('log4js');
var _ = require('underscore');

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

var fileSystemRoot = '/file';

//
// images: rewrite image URLs.
//
//  1. Assign top level url as follows:
//    if there are variants:
//      if one exists with a cached URL: (todo - for now NO cached URLs)
//        url = choose cached version of largest variant with a cached version.
//      else
//        url = url of largest variant.
//    else
//      url = path.join(fileSystemRoot, rawRep.path);
//
//  2. For each variant: (todo - for now NO cached URLs)
//    if cached version exists:
//      variant.url = cached URL.
//
var images = function(transformedRep, rawRep) {

  var lp = logPrefix.replace('.js: ', '.image: ');

  if (_.has(transformedRep, 'variants') && _.isArray(transformedRep) && transformedRep.length) {

    logger.debug(lp + 'Rep. has variants...');

    var largestVariant = undefined;
    _.each(transformedRep.variants, function(variant) {
      if (largestVariant === undefined) {
        largestVariant = variant;
      }
      else if ((variant.size.width * variant.size.height) > (largestVariant.size.width * largestVariant.size.height)) {
        largestVariant = variant;
      }
    });
    if (largestVariant) {
      transformedRep.url = largestVariant.url;
    }
  }
  else {
    transformedRep.url = path.join(fileSystemRoot, rawRep.path);
    logger.debug(lp + 'Rep. has NO variants, url set to - ' + transformedRep.url);
  }

  return transformedRep;
};

module.exports = {

  images: images,

  importers: function(transformedRep) {
    return transformedRep;
  },

  importersImages: function(transformedRep, rawRep) {

    var lp = logPrefix.replace('.js: ', '.importersImages: ');

    logger.debug(lp + 'Transforming importer w/ id - ' + transformedRep.id);

    for (var i = 0; i < transformedRep.images.length; ++i) {
      var transformed = transformedRep.images[i];
      var raw = rawRep.images[i];

      images(transformed, raw);
    }

    return transformedRep;
  }

};

