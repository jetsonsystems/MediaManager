'use strict';
var  fs  = require('fs')
    ,gm  = require('gm')
    ,cs  = require('./checksum')
    ,Image  = require('./Image')
    ,nano = require('nano')
    ,Step = require('step')
    ,_    = require('underscore')
    ,uuid = require('node-uuid')
    ,img_util = require('./image_util');
;

var config = {
  db: {
    host: "localhost",
    port: 5984,
    name: ""
  }
};

exports.config = config;

var checkConfig = function() {
  console.log('plm-image/ImageService: Checking config - ' + JSON.stringify(config) + '...');
  if (!config.db.name) {
    throw "plm-image/ImageService: ImageService.config.db.name must contain a valid database name!";
  }
  console.log('plm-image/ImageService: Config OK...');
};

// returns a db connection
var _db = function () {
  return nano('http://' + config.db.host + ':' + config.db.port + '/' + config.db.name);
}

// hide Object Id  generation lib inside private method
var _genOid = function() {
  return uuid.v4();
}

// checks to see whether an image with the given oid already exists,
// and passes current version to callback; this is useful in some cases
var findVersion = exports.findVersion = function (oid, callback) {
  var db = _db();
  Step(
    function () {
      console.log("getting version for oid: %j", oid);
      db.head(oid, this);
    },
    function (err, body, hdr) {
      if (err) {
        if (err.scope == 'couch' && err.status_code == 404) { 
        // there is no doc with that id, return null
        callback(null); return;
        } else { throw err;} // some other error
      }
      console.log("version is: %j", JSON.parse(hdr.etag));
      callback(JSON.parse(hdr.etag));
    }
  );
}


exports.import = function(imagePath, callback) 
{
  checkConfig();
  var db = nano('http://' + config.db.host + ':' + config.db.port + '/' + config.db.name);
  var image = new Image({path:imagePath, oid: _genOid()});

  // console.log("new image: " + JSON.stringify(image,null,"  "));

  Step(
    function () {
      console.log("calculating checksum...");
      cs.gen(fs.createReadStream(imagePath), this);
    },

    function (ss) { 
      console.log("done with checksum: " + ss);
      image.checksum = ss;
      // console.log("checksumed image: " + JSON.stringify(image,null,"  "));
      this();
    },
    
    function () {
      console.log("parsing image file...");
      gm(fs.createReadStream(imagePath)).identify(this);
    },

    function (err, data) {
      console.log("populating image");
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      image.readFromGraphicsMagick(data);
      // console.log("parsed image: " + JSON.stringify(image,null,"  "));
      this(err, image);
    },

    /*
    function (err, anOid) {
      console.log("checking to see if file exists...");
      if (err) { callback(err); return; }
      findVersion(anOid, this);
    },

    function (version) {
      console.log("saving to db...");
      if (_.isString(version)) {
        image["_rev"] = version;
      }

      db.insert(image,image.oid,this);
    },
    */

    function (anImage) {
      console.log("saving to db...");
      db.insert(image,image.oid,this);
    },

    function (err, body, headers) {
      if (err) { callback(err); return; }
      // console.log("result from 'save': " + JSON.stringify(headers));
      console.log("result from 'save': " + JSON.stringify(body));
      console.log("saving original...");
      var attachName = _.last(imagePath.split('/'));
      
      fs.createReadStream(imagePath).pipe(
        db.attachment.insert(
          body.id, 
          attachName,
          null,
          'image/'+image.format,
          {rev: body.rev}, callback)
      );
    }
  );
};

