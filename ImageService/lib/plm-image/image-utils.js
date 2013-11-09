'use strict';
var _ = require('underscore');

// Derives a desired new size { width: x, height: y} based on a size spec and the size of an image;
// if only the width or height is provided, the new size will fit to that width or height and
// preserve the aspect ratio; if both width and height are provided, the new size will be
// exactly as requested, which may distort the image
function fitToSize(size, newSize) {
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
exports.fitToSize = fitToSize;


// utility class that computes the new size for an image so that its longest size will be numPix
// long; so for example, if the image is 300x150 and numPix is 100, the computed image size will be
// 100x50, and if the image is 150x300 it will be 50x100
function fitToSquare(size, numPix) 
{
  var aspect_ratio = exports.aspectRatio(size);
  return (aspect_ratio >= 1) ? 
    exports.fitToWidth(size, numPix) : exports.fitToHeight(size, numPix);
}
exports.fitToSquare = fitToSquare;


// returns a new size that preserves the aspect ratio based on the pixel height
function fitToHeight(size, pixHeight) {
  var out = {};
  out.width  = Math.round( pixHeight*exports.aspectRatio(size) );
  out.height = pixHeight;
  return out;
}
exports.fitToHeight = fitToHeight;


function fitToWidth(size, pixWidth) {
  var out = {};
  out.width  = pixWidth;
  out.height = Math.round(pixWidth/aspectRatio(size));
  return out;
}
exports.fitToWidth = fitToWidth;


function aspectRatio(size) {
  if (_.isObject(size)) {
    if (_.isNumber(size.width) && _.isNumber(size.height) && size.width > 0 && size.height > 0) {
      return (size.width / size.height);
    }
  }
  throw "size is not properly defined";
}
exports.aspectRatio = aspectRatio;

