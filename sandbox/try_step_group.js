var Step = require('step');

var async = function(order, callback) {
  var timeout = Math.round(Math.random() * 1000);
  setTimeout( function() { callback(null, 'a-'+order); }, timeout);
}

Step (
  function doAll() {
    var group = this.group();
    for (var i = 0; i < 10; i++) {
      var j = i*2;
      async(j, group());
    }
  },
  function finalize(err) {
    console.log('done.arguments: ');
    console.log(arguments);
  }
);

