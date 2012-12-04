'use strict';
var iu = require('../lib/plm-image/image_util');

var new_size = iu.fitToSquare({width: 300, height: 150}, 100);
console.log("new size is %j", JSON.stringify(new_size));

new_size = iu.fitToSquare({width: 150, height: 300}, 100);
console.log("new size is %j", JSON.stringify(new_size));

new_size = iu.fitToSquare({width: 167, height: 300}, 100);
console.log("new size is %j", JSON.stringify(new_size));
