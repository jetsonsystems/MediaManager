'use strict';
var _ = require('underscore');
require('mootools');


// Wrapper class that stores a timestamp for the time at which an event is emitted
// -------------------------------------------------------------
module.exports = new Class (
{
  initialize : function(arg)
  {
    this.class_name = 'plm.Event';
    this.emitted_at = new Date();
    this.data = arg;
  }
});
