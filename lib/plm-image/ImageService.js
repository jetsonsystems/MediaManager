'use strict';
var  
   _   = require('underscore')
  ,async = require('async')
  ,cs  = require('./checksum')
  ,fs  = require('fs')
  ,gm  = require('gm')
  ,Image = require('./Image')
  ,img_util = require('./image_util')
  ,moment   = require('moment')
  ,nano  = require('nano')
  ,step  = require('step')
  ,util  = require('util')
  ,uuid  = require('node-uuid')
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

// map used to store all private functions
var priv = {};

// private final constants
var
  IMG_DESIGN_DOC = 'plm-image'
  ,VIEW_BY_CREATION_TIME    = 'by_creation_time'
  ,VIEW_BY_OID_WITH_VARIANT = 'by_oid_with_variant'
;


// call this at initialization time to check the db config and connection
exports.checkConfig = function checkConfig(callback) {
  console.log('plm-image/ImageService: Checking config - ' + JSON.stringify(config) + '...');

  if (!config.db.name) {
    throw "plm-image/ImageService: ImageService.config.db.name must contain a valid database name!";
  }

  var server = nano('http://' + config.db.host + ':' + config.db.port);
  server.db.get(config.db.name, callback);
};

// returns a db connection
priv.db = function db() {
  return nano('http://' + config.db.host + ':' + config.db.port + '/' + config.db.name);
}


priv.genOid = function genOid() {
  return uuid.v4();
}


// checks to see whether an image with the given oid already exists,
// and passes current version to callback; this is useful in some cases
exports.findVersion = function findVersion(oid, callback) {
  var db = priv.db();
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
exports.save = function save(anImgPath, callback, options) 
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
exports.parseAndTransform = function parseAndTransform(anImgPath, options, callback) 
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

  // var origOid = '';

  var that = this;

  step(
    function() {
      parseImage(anImgPath, this);
    },

    function(err, theImgMeta, theImgPath) {
      if (err) { throw err;}

      // we'll need this to reference the original in the variant's metadata
      // origOid = theImgMeta.oid;

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
          // variant.orig_id = origOid;
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
function transform(anImgMeta, variant, callback) 
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
        var tmp_file_name = config.workDir + '/plm-' + anImgMeta.oid + '-' + variant.name;
        gmImg.write(tmp_file_name, function(err) {
        if (err) next(err);
        next(null, tmp_file_name);
        });
      },

      function(aTmpFileName, next){
        // parseImage(variant.name, gm(fs.createReadStream(aTmpFileName)), next);
        parseImage(aTmpFileName, next);
      }
    ], 

    // called after waterfall ends
    function(err, theVariantMeta, theVariantPath){ 
      theVariantMeta.orig_id    = anImgMeta.oid;

      // timestamp the variants the same as the original, in order to properly sort originals and
      // variants when searching by creation date (this is a couchdb-specific tweak)
      theVariantMeta.created_at = anImgMeta.created_at;
      theVariantMeta.updated_at = anImgMeta.updated_at;

      // console.log('done processing variant: %j', JSON.stringify(theVariantMeta));
      console.log('done processing variant: %j', theVariantMeta);
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
exports.parseImage = function parseImage(anImgPath, callback) {
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


function persistMultiple(aryPersist, aryResult, callback)
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
function persist(persistCommand, callback) 
{
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


exports.show = function show(oid, callback, options) {
  var db = priv.db();
  var imgOut = {};

  // the view below sorts images by:
  //  - oid, 
  //  - whether they are originals or variants, and 
  //  - width
  //
  // this returns rows in the following order:
  // - original first
  // - variants in ascending size
  db.view(IMG_DESIGN_DOC, VIEW_BY_OID_WITH_VARIANT,
    { 
      startkey: [oid, 0, 0]      // 0 = original -  0 = min width
      ,endkey:  [oid, 1, 999999] // 1 = variant  -  999999 = max width
      ,include_docs: true
    }, 
    function(err, body) {
      console.log("Displaying an image and its variants using view '%j'", VIEW_BY_OID_WITH_VARIANT);

      if (!err) {

        imgOut = new Image(body.rows[0].doc);
        if (body.rows.length > 0) {
          for (var i = 1; i < body.rows.length; i++) {
            // console.log('oid %j - size %j - orig_id',row.doc.oid, row.doc.geometry, row.doc.orig_id);
            imgOut.variants.push( new Image(body.rows[i].doc) );
          }
        }
        callback(null, imgOut);

      } else {
        callback("error retrieving image with oid '" + oid + "': " + err);
      };
    }
  );
}


/* main image finder method */
exports.index = function index( callback, options ) {
  console.log("options: " + util.inspect(options));

  // TODO: 
  //  - The use cases below need to be expanded
  //  - Need to define paging options, and paging impl

  if (options && options.created) {
    exports.findByCreationTime( options.created, callback, options );
  } else {
    // TODO: this is temporary, returns all images sorted by creation time
    exports.findByCreationTime( null, callback, options);

  }
}


exports.findByCreationTime = function findByCreationTime( criteria, callback, options ) {

  var db = priv.db();
  var aryImgOut = []; // images sorted by creation time
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {}; // 
  // var theStartKey = [];
  // var theEndKey;

  var view_opts = {
    startkey: []
    ,include_docs: true
  };

  console.log("criteria: " + util.inspect(criteria));

  if (_.isArray(criteria)) {
    view_opts.startkey = priv.date_to_array(criteria[0]);
    view_opts.endkey   = priv.date_to_array(criteria[1]);
  } else if ( _.isString(criteria) ) {
    // TODO handle the case when only a single date is passed
  } else {
    // throw "Invalid Argument Exception: findByCreationTime does not understand options.created argument:: '" + criteria + "'";
  }

  // console.log("startkey: " + util.inspect(theStartKey));
  // console.log("end: " + util.inspect(theEndKey));

  console.log("view_opts: " + util.inspect(view_opts));

  db.view(IMG_DESIGN_DOC, VIEW_BY_CREATION_TIME, view_opts,
    /*
    { 
      startkey: theStartKey
      ,endkey:  theEndKey
      ,include_docs: true
    }, 
    */
    function(err, body) {
      console.log("Finding images and their variants using view '%j'", VIEW_BY_CREATION_TIME);

      if (!err) {

        // TODO: factor this out into a function that maps the body.rows collection 
        // into the proper Array of Image originals and their variants
        for (var i = 0; i < body.rows.length; i++) {
          // console.log(util.inspect(body.rows[i]));
          anImg = new Image(body.rows[i].doc);
          
          if ( anImg.isOriginal()) {
            imgMap[anImg.oid] = anImg;
            aryImgOut.push(anImg);
          } else {
            // if the image is a variant, add it to the original's variants array
            if (_.isObject(imgMap[anImg.orig_id])) {
              imgMap[anImg.orig_id].variants.push(anImg);
            } else {
              console.log("Warning: found variant image without a parent %j", anImg);
            }
          }
        }
        callback(null, aryImgOut);

      } else {
        callback("error in findByCreationTime with options '" + options + "': " + err);
      };
    }
  );

}

/**
 * Utility method that converts a Date instance into an array of ints representing:
 *
 * 0 Full Year (2012)
 * 1 Month (where January = 1)
 * 2 Day of Month
 * 3 Hours in military time (11 pm = 23)
 * 4 Minutes
 * 5 Seconds
 * 6 Millis
 *
 */
priv.date_to_array = function date_to_array(aDate) {

  // console.log("date_to_array arg: " + util.inspect(aDate));

  if (_.isString(aDate)) {
    aDate = moment(aDate, 'YYYYMMDD').toDate();
  }
  
  if (!_.isDate(aDate)) {
    throw "Invalid Argument Exception: argument is not a Date, or unable to parse string argument into a Date";
  }

  // console.log("date_to_array arg: " + util.inspect(aDate));

  return [
    aDate.getFullYear()
    ,aDate.getMonth()+1
    ,aDate.getDate()
    ,aDate.getHours()
    ,aDate.getMinutes()
    ,aDate.getSeconds()
    ,aDate.getMilliseconds()
  ]
}

// export all functions in pub map
/*
for (var func in pub) {
  if(_.isFunction(pub[func])) {
    exports[func] = pub[func];
  }
}
*/
