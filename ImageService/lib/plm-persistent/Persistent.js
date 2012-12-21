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

    if (_.isObject(args)) { 
         
      // a unique object/instance id
      if (_.isString(args.oid)) { this.oid = args.oid;}

      // a revision id for this instance
      // if (_.isString(args.rev)) { this.rev = args.rev;}
    }

    if (_.isObject(args) && _.isDate(args.created_at)) { 
      this.created_at = args.created_at;
    } else {
      this.created_at = new Date();
    }

    if (_.isObject(args) && _.isDate(args.updated_at)) { 
      this.updated_at = args.updated_at;
    } else {
      this.updated_at = this.created_at;
    }

    // a field intended to be private that stores storage-specific metadata
    this._storage  = {};
  }
});
