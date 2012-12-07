'use strict';
var _ = require('underscore');
require('mootools');
var Persistent = require('../plm-persistent');

// Image represents basic information on an image
// ----------------------------------------------
// exports.Image = new Class(
module.exports = new Class(
{
  Extends: Persistent,

  initialize : function(args)
  {
    this.parent(args);
    this.class_name = 'plm.Image'; 

    this.orig_id  = '';
    this.path     = '';
    this.format   = '';
    this.geometry = '';
    this.size     = '';
    this.type     = '';
    this.depth    = '';
    this.filesize = '';
    this.checksum = '';
    this.variants = [];
    this.metadata_raw = {};

    // a field intended to be private that stores storage-specific metadata
    this._storage  = {};

    
    if (_.isObject(args)) {
      this.path     = args.path;
      this.format   = args.format;
      this.geometry = args.geometry;
      this.size     = args.size;
      this.type     = args.type;
      this.depth    = args.depth;
      this.filesize = args.filesize;
      this.checksum = args.checksum;
      this.metadata_raw = args.metadata_raw;
      if(_.isString(args.orig_id)) {this.orig_id  = args.orig_id;}
    }
  },

  isVariant: function isVariant() {
    return  _.isString(this.orig_id) && this.orig_id !== '' ;
  },

  isOriginal: function isOriginal() {
    return !this.isVariant();
  },

  // returns a sanitized cloned instance without extraneous fields,
  // suitable for saving or encoding into json
  toJSON : function() {
    var out = Object.clone(this);
    // these two are added by mootools
    delete out.$caller;
    delete out.caller;

    // do not stringify variants, these have to be stringified individually via this.variants;
    // also, variants are not stored in couch with the original doc
    delete out.variants;

    //TODO: output date/timestamps as: "2009/05/25 06:10:40 +0000" ?
    
    // cloning will cause functions to be saved to couch if we don't remove them
    for (var prop in out) {
      if ( prop.indexOf("_") === 0 || _.isFunction(out[prop]) ) {
        delete out[prop];
      }
    }
    return out;
  },

  // populate this Image object based on the output of GraphicsMagick.identify()
  readFromGraphicsMagick : function(obj)
  {
    if (_.isObject(obj)) {
      this.format   = obj.format;
      this.geometry = obj.Geometry;
      this.size     = obj.size;
      this.filesize = obj.Filesize;
      this.metadata_raw = obj;
    }
  }
}); 

// var img = new Image();
// console.log('img: ' + JSON.stringify(img));
