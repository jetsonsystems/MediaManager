'use strict';
var _ = require('underscore');
require('mootools');
var Persistent = require('../plm-persistent');

// ImportBatch represents basic information on an batch import process
// ----------------------------------------------
module.exports = new Class(
{
  Extends: Persistent,

  initialize : function(args)
  {
    this.parent(args);
    this.class_name = 'plm.ImportBatch'; 

    // stores the time at which the batch ended processing
    this.ended_at = undefined;

    // transient field that stores an array of tuples such as: 
    //   {path: 'someImagePath', format: 'jpg'}
    // for the images that this batch should import
    this.images_to_import = [];

    // transient map of the images that were successfully imported in this batch, keyed by path; 
    // this field not persisted, images will contain a reference to the batch's oid instead
    this.images = {}; 

    // map of errors in this batch, keyed by path
    // not persisted, errs will be persisted individually and contain a reference to batch oid
    this.errs = {};

    // if this import batch resulted from a batch import of a folder, 
    // stores the root path of the import
    this.root_path = '';

    var that = this;

    if (_.isObject(args)) {
      _.each(args, function(value, key) {
        if (value) { that[key] = value; }
      });
    }
  },

  // returns the time at which the batch began - synonym for created_at
  getBeganAt: function getBeginAt() {
    return this.created_at;
  },

  getEndedAt: function getEndedAt() {
    return this.ended_at;
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
    delete out.images;
    delete out.errs;

    // cloning will cause functions to be saved to couch if we don't remove them
    for (var prop in out) {
      if ( prop.indexOf("_") === 0 || _.isFunction(out[prop]) ) {
        delete out[prop];
      }
    }
    return out;
  }

}); 

// var img = new Image();
// console.log('img: ' + JSON.stringify(img));
