'use strict';

var 
  async = require('async')
  ,util = require('util')
;

var collection = [
   { oid: 'aoeustnh', mult: 1 }
  ,{ oid: 'al09gau', mult: 2 }
  ,{ oid: 'lstb25ah', mult: 3}
  ,{ oid: 'srcxhtboh', mult: 4 }
  ]
;

var results1 = [], results2 = [];

function process(element, callback) {
  setTimeout(function() { 
    if (element.mult === 3) {callback("reached number 3"); return;}
    callback(null, {oid: element.oid, result: 2*element.mult });
  }, 2000 * (1 + Math.random()));
}

function iterator1(element, next) {
  console.log('processing :%j', JSON.stringify(element));
  process(element, function(err,result) {
    if(err) next(err);
    results1.push(result);
    next();
  });
}

function iterator2(element, next) {
  console.log('processing :%j', JSON.stringify(element));
  process(element, function(err, result) {
    if(err) next(err);
    results2.push(result);
    next();
  });
}


function callback1(err) {
  if (err) console.log("encountered error: %j", err);
  console.log("results1: %j", util.inspect(results1));
}

function callback2(err) {
  if (err) console.log("encountered error: %j", err);
  console.log("results2: %j", util.inspect(results2));
}

console.log("calling async.forEach");
async.forEach(collection, iterator1, callback1);

console.log("calling async.forEachSeries");
async.forEachSeries(collection, iterator2, callback2);
