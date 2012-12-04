'use strict';

/* 
 * if we had declared in Persistent.js:
 *   exports.Persistent = new Class (...)
 * then we would instantiate a class like so:
 */

// var _Persistent = require('./lib/plm-persistent');
// var p = new _Persistent.Persistent();

/* or */

// var p = new (require('./lib/plm-persistent')).Persistent();

/* instead, because in Persistent.js we have:
 *   module.exports = new Class (...)
 * then we can do:
 */

// var 
//   Persistent = require('../lib/plm-persistent'),
//   p = new Persistent()
// ;

/* or */

var p = new (require('../lib/plm-persistent'))();

console.log('stringify p:' + JSON.stringify(p));
