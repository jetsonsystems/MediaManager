'use strict';
var _ = require('underscore');
require('mootools');


// Persistent is a parent class/interface for persistent objects;
// -------------------------------------------------------------
module.exports = new Class (
{
  /* Implements: [process.EventEmitter], */

  initialize : function(args)
  {
    this.class_name = 'plm.Persistent'; // override in extending class

    if (_.isObject(args) && _.isString(args.uuid)) { this.uuid = args.uuid;}

    if (_.isObject(args) && _.isDate(args.created_at)) { 
      this.created_at = args.created_at;
    } else {
      this.created_at = new Date();
    }

    if (_.isObject(args) && _.isDate(args.updated_at)) { 
      this.updated_at = args.updated_at;
    } else {
      this.updated_at = new Date();
    }
  }
});
