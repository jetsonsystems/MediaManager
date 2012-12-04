var cradle = require('cradle');
var db = new(cradle.Connection)().database('plm_staging');

db.save('some_image.png',
{
	name: 'some_image.png',
	path: '/path/to/image',
	width: 245,
	height: 123,
	format: 'png'
}, 
function(err,res) {
	if (err) {
		console.log(err)
	} else {;
		console.log(res);
	}
});
