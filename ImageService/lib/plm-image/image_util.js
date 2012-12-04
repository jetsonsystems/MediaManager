'use strict';
var _ = require('underscore');

// Derives a desired new size { width: x, height: y} based on a size spec and the size of an image;
// if only the width or height is provided, the new size will fit to that width or height and
// preserve the aspect ratio; if both width and height are provided, the new size will be
// exactly as requested, which may distort the image
exports.fitToSize = function (size, newSize) {
  if (newSize.width > 0) {
    if (newSize.height > 0) {
      return newSize;
    } else {
      return exports.fitToWidth(size, newSize.width);
    }
  } else if (newSize.height > 0) {
    return exports.fitToHeight(size, newSize.height);
  } else {
    throw "fitToSize new size is improperly defined";
  }
}

// utility class that computes the new size for an image so that its longest size will be numPix
// long; so for example, if the image is 300x150 and numPix is 100, the computed image size will be
// 100x50, and if the image is 150x300 it will be 50x100
exports.fitToSquare = function (size, numPix) 
{
  /*
  if (_.isObject(size)) {
    if (_.isNumber(size.width) && _.isNumber(size.height) && size.width > 0 && size.height > 0) {
      var aspect_ratio = size.width / size.height;
      var out = {};
      if (aspect_ratio >= 1) {
        out.width  = numPix;
        out.height = Math.round(numPix/aspect_ratio);
      } else {
        out.width  = Math.round(numPix*aspect_ratio);
        out.height = numPix;
      }
      return out;
    }
  } 
  throw "size is not properly defined";
  */
  var aspect_ratio = exports.aspectRatio(size);
  return (aspect_ratio >= 1) ? 
    exports.fitToWidth(size, numPix) : exports.fitToHeight(size, numPix);
}

// returns a new size that preserves the aspect ratio based on the pixel height
exports.fitToHeight = function (size, pixHeight) {
  var out = {};
  out.width  = Math.round( pixHeight*exports.aspectRatio(size) );
  out.height = pixHeight;
  return out;
}

exports.fitToWidth = function (size, pixWidth) {
  var out = {};
  out.width  = pixWidth;
  out.height = Math.round(numPix/aspect_ratio);
  return out;
}

exports.aspectRatio = function (size) {
  if (_.isObject(size)) {
    if (_.isNumber(size.width) && _.isNumber(size.height) && size.width > 0 && size.height > 0) {
      return (size.width / size.height);
    }
  }
  throw "size is not properly defined";
}

