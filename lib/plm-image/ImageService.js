'use strict';
var  
   _   = require('underscore')
  ,fs  = require('fs')
  ,gm  = require('gm')
  ,cs  = require('./checksum')
  ,nano  = require('nano')
  ,step  = require('step')
  ,uuid  = require('node-uuid')
  ,util  = require('util')
  ,async = require('async')
  ,Image = require('./Image')
  ,img_util = require('./image_util')
;

var config = {
  db: {
    host: "localhost",
    port: 5984,
    name: ""
  },
  workDir : '/var/tmp'
};

exports.config = config;

/* map used to store all private functions */
var priv = {};
var pub  = {};

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


priv.genOid = function() {
  return uuid.v4();
}

// checks to see whether an image with the given oid already exists,
// and passes current version to callback; this is useful in some cases
exports.findVersion = function (oid, callback) {
  var db = _db();
  step(
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


/** the main ingestion function */
exports.save = function(anImgPath, callback, options) 
{
  step(
    function() {
      parseAndTransform(anImgPath, options, this);
    },
    // function(err, imgData, imgStream) {
    function(err, aryPersist) {
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      // persist( {data: imgData, stream: imgStream }, callback);
      console.log("calling persistMultiple...");
      persistMultiple(aryPersist, null, callback);
    }
  );
} // end save


/*
 * When provided with an image path, an optional stream, and a set of transforms, 
 * instantiate a gm object, and pass it to parseImage for each variant
 * Invokes the callback with an array of ImageData/Stream to persist
 */
var parseAndTransform = exports.parseAndTransform = function(anImgPath, options, callback) 
{
  if (!_.isFunction(callback)) throw err("parseAndTransform is not very useful if you don't provide a valid callback");

  var saveOriginal = options && _.has(options, 'saveOriginal') ? 
    options.saveOriginal : true;

  var variants     = options && _.has(options, 'desiredVariants') && _.isArray(options.desiredVariants) ? 
    options.desiredVariants : [];

  // var variant = variants[0];

  /*
  var imgStream = (_.isObject(anImgStream)) ?  
    anImgStream : fs.createReadStream(anImgPath);
  */
  // var imgStream = fs.createReadStream(anImgPath);

  var aryPersist = [];

  var origOid = '';

  var that = this;

  step(
    function() {
      parseImage(anImgPath, this);
    },

    function(err, theImgMeta, theImgPath) {
      if (err) { throw err;}

      // we'll need this to reference the original in the variant's metadata
      origOid = theImgMeta.oid;

      if (saveOriginal) {
        aryPersist.push({ data: theImgMeta, stream: theImgPath });
      } else {
        aryPersist.push({ data: theImgMeta });
      }

      if (!_.isObject(variants[0])) {
        // we are done
        console.log("no variant to process...");
        callback(null, aryPersist);
      }
      else { 

        var iterator = function(variant, next) {
          variant.orig_id = origOid;
          transform(theImgMeta, variant, function(err, theVariantData, theVariantPath) {
            if (err) { next(err); } 
            console.log('theVariantPath is: %j', theVariantPath);
            aryPersist.push({ data: theVariantData, stream: theVariantPath, isTemp: true });
            // console.log('aryPersist.length: %j', aryPersist.length);
            console.log("returning the variant...");
            next();
          });
        };

        async.forEachSeries( variants, iterator, function(err) {
          if (err) callback(err);
          console.log("done generating all the variants...");
          callback(null, aryPersist);
        });
      }
    }
  );
}

/** returns theImgData, theImgStream */
var transform = function(anImgMeta, variant, callback) 
{
  var gmImg = gm(anImgMeta.path);
  async.waterfall(
    [
      function(next){
        //TODO: need more validation around variant specs
        if (variant.width || variant.height) {
          var newSize = img_util.fitToSize(anImgMeta.size, { width: variant.width, height: variant.height });
          gmImg.resize(newSize.width, newSize.height);
        } 
        console.log('generating bits for variant...');
        // gmImg.stream(variant.format, next);
        var tmp_file_name = config.workDir + '/plm-' + variant.orig_id + '-' + variant.name;
        gmImg.write(tmp_file_name, function(err) {
        if (err) next(err);
        next(null, tmp_file_name);
        });
      },

      function(aTmpFileName, next){
        // parseImage(variant.name, gm(fs.createReadStream(aTmpFileName)), next);
        parseImage(aTmpFileName, next);
      },
    ], 

    // called after waterfall ends
    function(err, theVariantMeta, theVariantPath){ 
      theVariantMeta.orig_id = variant.orig_id;
      console.log('done processing variant: %j', JSON.stringify(theVariantMeta));
      callback(err, theVariantMeta, theVariantPath); 
    }
  );
}


/*
 * Private method that takes a string path or ReadStream to an image on the file system, than:
 *
 * - parses the image, 
 * - computes its checksum,
 * - instantiates an Image object, and 
 * - invokes callback(err, imgData, imgStream) 
 *
 * where:
 *
 * - imgData: an Image object containing the image's metadata
 * - imgStream: a ReadStream suitable for piping to a storage device or a file on the filesystem
 *
 * TODO: move this method to an ImageProcessor instance
 * TODO: need a version of this method that returns the gm object so that variants can be generated by
 * re-using the original object
 */
var parseImage = exports.parseImage = function (anImgPath, callback) {
  if (!_.isFunction(callback)) throw err("parseImage is not very useful if you don't provide a valid callback");

  var imageMeta = new Image({path:anImgPath, oid: priv.genOid()});
  var gmImg   = gm(fs.createReadStream(anImgPath));

  step(
    function () {
      console.log("parsing image file...");
      // the 'bufferStream: true' parm is critical, as it buffers the file in memory 
      // and makes it possible to stream the bits repeatedly
      gmImg.identify({bufferStream: true},this);
    },

    function (err, data) {
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      console.log("populating image");
      imageMeta.readFromGraphicsMagick(data);
      // console.log("parsed image: %", util.inspect(imageMeta));
      // this would fail if 'bufferStream' above were not true
      gmImg.stream(this);
    },

    function (err, anImgStream, anErrStream) {
      console.log("calculating checksum...");
      cs.gen(anImgStream, this);
    },

    function (aString) { 
      console.log("done with checksum: " + aString);
      imageMeta.checksum = aString;
      // console.log("checksumed image: " + JSON.stringify(imageMeta,null,"  "));
      // gmImg.stream(this);
      callback(null, imageMeta, anImgPath);
    }
  );
} // end parseImage


var persistMultiple = function (aryPersist, aryResult, callback)
{
  // handle empty aryPersist
  if ( !(aryPersist instanceof Array) || aryPersist.length === 0) 
  {
    var err = 'persistMultiple cowardly refused to persist an empty array of persist instructions';
    console.log(err);
    if (callback instanceof Function) {
      callback(err);
    }
    return;
  }

  // if a results array has not been passed to us, create a new one;
  // passing a result array is helpful for aggregating the results 
  // of multiple invocations of this method
  if (!_.isArray(aryResult)) aryResult = [];

  function iterator(element, next) {
    persist(element, function(err, image) {
      // console.log("persisted img: %j", util.inspect(image));
      aryResult.push( err ? err : image );
      next();
    });
  }

  console.log('about to call forEachSeries');
  async.forEachSeries(aryPersist, iterator, function(err) {
    if(err) console.log("error happened saving images");
    console.log('done saving images, call persistMultiple callback');
    callback(null, aryResult);
  });
}


/*
 * Takes an Image object and a readstream from an image, and saves them to persistent storage
 *
 * This should be moved to a DAO class that is couchdb-specific
 */
var persist = function (persistCommand, callback) 
{
  checkConfig();
  var 
    db = nano('http://' + config.db.host + ':' + config.db.port + '/' + config.db.name)
    ,imgData   = persistCommand.data
  ;

  step(

    function () {
      console.log("saving to db...");
      db.insert(imgData, imgData.oid, this);
    },

    function (err, body, headers) {
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      // console.log("result from 'save': " + JSON.stringify(headers));
      console.log("result from 'save': " + JSON.stringify(body));

      imgData._storage.type = 'couchdb';
      imgData._storage._id  = body.id;
      imgData._storage._rev = body.rev;

      // console.log("saved image: " + JSON.stringify(imgData,null,"  "));

      if (_.isString(persistCommand.stream)) {
        console.log("streaming image bits to storage device from %j", persistCommand.stream);
        var imgStream = fs.createReadStream(persistCommand.stream);

        var attachName = _.last(imgData.path.split('/'));

        //console.log("imgData: %j", util.inspect(imgData));
        //console.log("attachName: %j", attachName);
        console.log("stream: %j", util.inspect(imgStream));

        try {
        imgStream.pipe(
          db.attachment.insert(
            imgData.oid,
            attachName,
            null,
            'image/'+imgData.format,
            {rev: imgData._storage._rev}, this)
        );  
        }
        catch(e) { console.log("streaming error: %j", util.inspect(e));}
        // console.log("stream after: %j", util.inspect(imgStream));
      } else {
        // we are done
        // TODO this won't work
        callback(null, imgData);
        return;
      }
    },

    function(err, body, headers) {
      console.log("returning saved results");
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      imgData._storage._rev = body.rev;

      // clean-up work directory if this was a temp file generated in the workDir
      if ( persistCommand.isTemp && _.isString(persistCommand.stream) ) {
        fs.unlink(persistCommand.stream, function(err) { 
          if (err) { console.log('warning: exception when deleting img from workDir: %j', err); } 
        });
      }
      callback(null, imgData);
    }
  );

} // end persist

// export all functions in pub map
/*
for (var func in pub) {
  if(_.isFunction(pub[func])) {
    exports[func] = pub[func];
  }
}
*/
