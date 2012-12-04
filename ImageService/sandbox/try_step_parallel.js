var Step = require('step');

var async = function(order, callback) {
  var timeout = Math.round(Math.random() * 1000);
  console.log('timeout %j is: %j', order, timeout);
  setTimeout( function() { callback(null, 'a-'+order); }, timeout);
}

Step (
  function doAll() {
    for (var i = 0; i < 10; i++) {
      var j = i*2;
      async(j,this.parallel());
    }
  },
  function finalize(err) {
    console.log('done.arguments: ');
    console.log(arguments);
  }
);

