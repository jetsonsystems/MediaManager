'use strict';
var uuid = require('node-uuid');

var i = 1;

while (i <= 10) {
  console.log("uuid #%j: %j",i, uuid.v4());
  i++;
}
