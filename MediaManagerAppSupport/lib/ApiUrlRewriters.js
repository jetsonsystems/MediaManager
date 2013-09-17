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

var config = require('MediaManagerAppConfig');
//
// fileCache: Must have been instantiated previously.
//
var fileCacheAlias = config.storage["file-cache"].alias;
var fileCache = require('MediaManagerStorage')().get('file-cache',
                                                     { singleton: true,
                                                       alias: fileCacheAlias
                                                     });

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var moduleName = './' + __filename.split('/').slice(-2).join('/').replace('.js', '');
var logPrefix = moduleName + ': ';

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

  var lp = logPrefix.replace(': ', '.image: ');

  if (_.has(transformedRep, 'variants') && _.isArray(transformedRep.variants) && transformedRep.variants.length) {

    var largestVariant = undefined;
    _.each(transformedRep.variants, function(variant) {

      var vPath = fileCache.getPath(variant.name,
                                    rawRep.oid,
                                    { type: 'full' });

      if (vPath) {
        variant.url = vPath;
      }
      else {
        logger.debug(lp + 'About to request file cache put for image w / id - ' + rawRep.oid + ', variant name - ' + variant.name + ', variant url - ' + variant.url);
        try {
          fileCache.putFromUrl(variant.url,
                               rawRep.oid,
                               { type: 'full' },
                               function(err, cachedPath) {
                                 if (err) {
                                   logger.error(lp + 'Error caching variant, image id - ' + rawRep.oid + ', variant name - ' + variant.name + ', error - ' + err);
                                 }
                                 else {
                                   logger.debug(lp + 'Cached variant for image w/ id - ' + rawRep.oid + ', variant name - ' + variant.name + ', cached path - ' + cachedPath);
                                 }
                               });
        }
        catch (e) {
          logger.error(lp + 'Error attempting put to file cache, error - ' + e);
        }
      }
      
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
  }

  return transformedRep;
};

module.exports = {

  images: images,

  importers: function(transformedRep) {
    return transformedRep;
  },

  importersImages: function(transformedRep, rawRep) {

    var lp = logPrefix.replace(': ', '.importersImages: ');

    logger.debug(lp + 'Transforming importer w/ id - ' + transformedRep.id);

    for (var i = 0; i < transformedRep.images.length; ++i) {
      var transformed = transformedRep.images[i];
      var raw = rawRep.images[i];

      images(transformed, raw);
    }

    return transformedRep;
  }

};
