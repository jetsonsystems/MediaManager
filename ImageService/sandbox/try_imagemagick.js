var im = require('imagemagick');
im.readMetadata("clooawood.png", function(err, metadata) {
		if (err) throw err;
		console.log("metadata: " + metadata);
		console.log("stringify: " + JSON.stringify(metadata));
});
