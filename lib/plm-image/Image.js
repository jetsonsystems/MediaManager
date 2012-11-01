'use strict';
var _ = require('underscore');
require('mootools');
var Persistent = require('../plm-persistent');

// Image represents basic information on an image
// ----------------------------------------------
// exports.Image = new Class(
module.exports = new Class(
{
  // Extends: _Persistent.Persistent,
  Extends: Persistent,

  initialize : function(path, args) 
  {
    this.parent(args);
    this.class_name = 'plm.Image'; 

    if (_.isString(path)) { this.path = path; }

    if (_.isObject(args)) 
    {
      this.format   = args.format;
      this.geometry = args.Geometry;
      this.size     = args.size;
      this.type     = args.Type;
      this.depth    = args.depth;
      this.filesize = args.Filesize;
      this.checksum = args.checksum;
      this.metadata_raw = args;
    }
  }

}); 

// var img = new Image();
// console.log('img: ' + JSON.stringify(img));
