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
    this.batch_id = '';
    this.path     = '';
    this.name     = '';
    this.format   = '';
    this.geometry = '';
    this.size     = '';
    this.type     = '';
    this.filesize = '';
    this.checksum = '';
    this.variants = [];
    this.metadata_raw = {};

    // by default, we suppress display of raw metadata in toJSON,
    // set this to true prior to calling toJSON to expose the metadata
    this.exposeRawMetadata = false;

    
    if (_.isObject(args)) {

      if (!_.isString(args.orig_id) ) { delete args.orig_id; }
      if (!_.isString(args.batch_id)) { delete args.batch_id;}

      for (var key in args) {
        this[key] = args[key];
      }

      if (!this.name && this.path) {
        this.name = this.extractNameFromPath(this.path);
      }

      /*
      this.path     = args.path;
      this.name     = args.name;
      this.format   = args.format;
      this.geometry = args.geometry;
      this.size     = args.size;
      this.type     = args.type;
      this.filesize = args.filesize;
      this.checksum = args.checksum;
      this.metadata_raw = args.metadata_raw;

      if(_.isString(args.orig_id))  {this.orig_id  = args.orig_id;}
      if(_.isString(args.batch_id)) {this.batch_id = args.batch_id;}
      */

    }
  },

  isVariant: function isVariant() {
    return  _.isString(this.orig_id) && this.orig_id !== '' ;
  },

  isOriginal: function isOriginal() {
    return !this.isVariant();
  },

  /** 
   * class-level (static) utility method that retrieves a name attached to a path, assumes a unix
   * '/' path separator
   * TODO: enhance this so that it works on windows
   */
  extractNameFromPath: function extractNameFromPath(aPath) {
    var tokens = aPath.split("/");
    return tokens[tokens.length - 1];
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
    
    delete out.exposeRawMetadata;
    if (!this.exposeRawMetadata) { delete out.metadata_raw; }
    
    // cloning will cause functions to be saved to couch if we don't remove them
    var storage = this._storage;
    for (var prop in out) {
      if ( prop.indexOf("_") === 0 || _.isFunction(out[prop]) ) {
        delete out[prop];
      }
    }
    if (_.has(storage, 'rev')) {
      out._rev = storage.rev;
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
