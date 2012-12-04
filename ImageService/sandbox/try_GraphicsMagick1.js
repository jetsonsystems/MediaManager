var fs = require('fs'), gm = require('gm');

gm("clooawood.png").identify(function(err, data) {
		if (err) console.log(err);
		console.log(data);
});

console.log("---");

gm("/home/philippe/multimedia/pictures/2012/2011-10-31_10-11-14.jpg").identify(function(err, data) {
		if (err) console.log(err);
		console.log(data);
});
