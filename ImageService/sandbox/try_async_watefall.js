'use strict';

var 
  async = require('async')
  ,util = require('util')
;


var call_order = [];
async.waterfall([
    function(callback){
        call_order.push('fn1');
        setTimeout(function(){callback(null, 'one', 'two');}, 0);
    },
    function(arg1, arg2, callback){
        call_order.push('fn2');
        console.log("arg1 is: %j; arg2 is: %j", arg1, arg2);
        setTimeout(function(){callback(null, arg1, arg2, 'three');}, 25);
    },
    function(arg1, arg2, arg3, callback){
        call_order.push('fn3');
        console.log("arg1 is: %j; arg2 is: %j; arg3 is: %j", arg1, arg2, arg3);
        callback(null, 'four');
    },
    function(arg4, callback){
        call_order.push('fn4');
        console.log('call_order is: %j', util.inspect(call_order));
        callback(null, 'test');
    }
], function(err){
  console.log("done!");
});
