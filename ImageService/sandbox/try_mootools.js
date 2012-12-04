'use strict';
require('mootools');
var _ = require('underscore');

var Images = new Class({ 
  initialize: function() { 
    console.log("our object keys - " + JSON.stringify(_.keys(this._obj))); 
    console.log("our object keys - " + JSON.stringify(_.keys(this.__proto__._obj))); 
    console.log(this._obj.a);
  },
  _obj : { a: 'A', b: 'B' } 
});

var i = new Images();
console.log("our object keys - " + JSON.stringify(_.keys(i._obj))); 
console.log("our object keys - " + JSON.stringify(_.keys(i.__proto__._obj))); 
console.log(i._obj.a);
