'use strict';
var
  _     = require('underscore')
  ,strUtils = require('underscore.string')
  ,async = require('async')
  ,cs    = require('./checksum')
  ,dive  = require('dive')
  ,fs    = require('fs')
  ,gm    = require('gm')
  ,img_util = require('./image_util')
  ,log4js   = require('log4js')
  ,mime     = require('mime-magic')
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
  app: undefined,
  workDir : '/var/tmp',
  processingOptions : {
    genCheckSums: false,
    numJobs: 1  // number of image processing jobs to trigger in parallel during imports
  }
};

//
// Note any config is OK here now, as we don't use MediaManagerStore to actually talk
// to the DB. In the future we should require invoking ImageService as a function passing a config,
// where the returned instance will use that config.
//
var mmStorage = require('MediaManagerStorage')(config.db, {singleton: false});

exports.config = config;

var log = log4js.getLogger('plm.ImageService');
var nanoLog = log4js.getLogger('plm.ImageService.nano');

var nanoLogFunc = function(eventId, args) {
  var logStr = '';
  if (eventId) {
    logStr = 'event - ' + eventId;
  }
  if (args && args.length) {
    for (var i = 0; i < args.length; ++i) {
      try {
        logStr = logStr + ', ' + JSON.stringify(args[i]);
      }
      catch (e) {
        logStr = logStr + ', ' + args[i].toString();
      }
    }
  }
  nanoLog.debug(logStr);
};

// map used to store all private functions
var priv = {};

// private final constants
var
  IMG_DESIGN_DOC = 'plm-image'
  ,VIEW_BY_CTIME             = 'by_creation_time'
  ,VIEW_BY_CTIME_TAGGED    = 'by_creation_time_tagged'
  ,VIEW_BY_CTIME_UNTAGGED    = 'by_creation_time_untagged'
  ,VIEW_BY_OID_WITH_VARIANT  = 'by_oid_with_variant'
  ,VIEW_BY_OID_WITHOUT_VARIANT = 'by_oid_without_variant'
  ,VIEW_BATCH_BY_CTIME       = 'batch_by_ctime'
  ,VIEW_BATCH_BY_OID_W_IMAGE = 'batch_by_oid_w_image'
  ,VIEW_BY_TAG               = 'by_tag'
  ,VIEW_TRASH                = 'by_trash'

  ;

// a hashmap, keyed by oid, that caches importBatch objects while they are being processed
priv.batch_in_process = {};

priv.markBatchInit = function (anImportBatch) {
  priv.batch_in_process[anImportBatch.oid] = anImportBatch;
  anImportBatch.setStatus(anImportBatch.BATCH_INIT);
};

/*
 priv.markBatchStarted = function (anImportBatch) {
 anImportBatch.setStartedAt(new Date());
 };
 */

priv.markBatchComplete = function (anImportBatch)
{
  if (_.isObject(anImportBatch)) {
    if (_.has(priv.batch_in_process, anImportBatch.oid)) {
      delete priv.batch_in_process[anImportBatch.oid];
    }
    anImportBatch.setCompletedAt(new Date());
  } else {
    log.warn("Illegal Argument in markBatchComplete: '%s'", anImportBatch);
  }
};


// call this at initialization time to check the db config and connection
exports.checkConfig = function checkConfig(callback) {
  log.info('plm-image/ImageService: Checking config - ' + JSON.stringify(config) + '...');

  if (!config.db.name) {
    throw "plm-image/ImageService: ImageService.config.db.name must contain a valid database name!";
  }

  var server = nano(
    {
      url: 'http://' + config.db.host + ':' + config.db.port,
      log: nanoLogFunc
    });
  server.db.get(config.db.name, callback);
};

var dbServer = null;

// returns a db connection
priv.db = function db() {
  log.trace("priv.db: Connecting to data base, host - '%s' - port '%s' - db '%s'", config.db.host, config.db.port, config.db.name);
  dbServer = dbServer || nano(
    {
      url: 'http://' + config.db.host + ":" + config.db.port,
      log: nanoLogFunc
    }
  );
  return dbServer.use(config.db.name);
};


priv.genOid = function genOid() {
  return uuid.v4();
};


// checks to see whether an image with the given oid already exists,
// and passes current version to callback; this is useful in some cases
exports.findVersion = function findVersion(oid, callback) {
  var db = priv.db();
  step(
    function () {
      log.debug("getting version for oid: %j", oid);
      db.head(oid, this);
    },
    function (err, body, hdr) {
      if (err) {
        if (err.scope === 'couch' && err.status_code === 404) {
          // there is no doc with that id, return null
          callback(null); return;
        } else { throw err;} // some other error
      }
      log.info("Version of image '%s' is: %s", oid, hdr.etag);
      callback(JSON.parse(hdr.etag));
    }
  );
};

//
// Some helpers:
//  bulkDocFetch: Fetch a bunch of documents given a list of doc IDs.
//  fetchDocs: Fetch documents, optionally in batches.
//  runView: Run a view, and optionally including documents when running
//    the view, or separately fetching the documents.
//

//
// bulkDocFetch: Fetchs a set of documents.
//  Args:
//    docIds: Array of document IDs.
//    callback: Invoked as callbac(err, docs), where docs is an array of
//      the fetched documents.
//
//  Returns: The fetched docs in an array.
//
//  Essentially, does the equivalent of:
//
//    curl -d '{"keys":["bar","baz"]}' -X POST http://127.0.0.1:5984/foo/_all_docs?include_docs=true
//
var bulkDocFetch = function(docIds, callback) {
  if (docIds && _.isArray(docIds) && (docIds.length > 0)) {
    var db = priv.db();
    db.fetch({keys: docIds},
             {},
             function(err, body) {
               if (err) {
                 callback && callback('Error occurred fetching documents, error - ' + err);
               }
               else if (_.has(body, 'rows')) {
                 if (_.isArray(body.rows) && body.rows.length > 0) {
                   log.debug('bulkDocFetch: Fetched ' + body.rows.length + ' documents, first doc - ' + util.inspect(body.rows[0].doc));
                   var docs = _.pluck(body.rows, "doc");
                   callback && callback(null, docs);
                 }
                 else {
                   callback && callback('No documents were fetched.');
                 }
               }
             });
  }
  else {
    callback && callback('No documents were requested.');
  }
};

//
// fetchDocs: Fetch documents, optionally in batches.
//
//  Args:
//    docIds: Document IDs.
//    options:
//      batchSize: Batchsize to use. By default ALL documents will be fetched.
//      callback: callback(err, docs)
//
var fetchDocs = function(docIds, options) {
  options = options || {};
  if (!options.batchSize) {
    options.batchSize = docIds.length;
  }
  var callback = options.callback || undefined;
  var docs = [];
  var start = 0;
  async.whilst(
    function() { return start < docIds.length; },
    function(innerCallback) {
      var end = (start+options.batchSize<docIds.length)?start+options.batchSize:docIds.length;
      log.debug('fetchDocs: Fetching [' + start + ', ' + end + '].');
      var docIdsToFetch = docIds.slice(start, end);
      start = end;
      bulkDocFetch(docIdsToFetch, 
                   function(err, docsFetched) {
                     if (!err && docsFetched) {
                       log.debug('fetchDocs: Adding ' + docsFetched.length + ' documents to result set...');
                       docs.push.apply(docs, docsFetched);
                       log.debug('fetchDocs: Total documents fetched - ' + docs.length);
                     }
                     innerCallback(err);
                   });
    },
    function(err) {
      log.debug('fetchDocs: Finished fetching documents, fetched - ' + docs.length);
      callback && callback(err, docs);
    }
  );
};

//
// runView: Run a view, and optionally including documents when running
//  the view, or separately fetching the documents.
//
//  Args:
//    designDoc
//    viewName
//    options:
//      toReturn: What should be returned:
//
//        'ids': document ids
//        'docs': documents should be returned.
//
//        default: 'ids'
//
//      viewOptions: Options to pass to the view, ie: startkey, etc.
//      fetchDocs: When toReturn is 'docs', fetch the docs separately. include_docs = true is NOT passed to the view.
//      fetchDocsBatchSize: When fetchDocs is specified, optionally specify a batchsize.
//      callback:
//
var runView = function(designDoc, viewName, options) {
  log.debug('runView: design doc. - ' + designDoc + ', view name - ' + viewName + ', options ' + util.inspect(options));
  options = options || {};

  if (!options.toReturn) {
    options.toReturn = 'ids';
  }
  
  var callback = options.callback || undefined;

  if (!callback) {
    log.debug('runView: no callback!');
  }

  var viewOptions = options.viewOptions || {};

  if (options.toReturn === 'ids') {
    viewOptions.include_docs = false;
  }
  else {
    if (options.fetchDocs) {
      viewOptions.include_docs = false;
    }
    else {
      viewOptions.include_docs = true;
    }
  }

  async.waterfall(
    [
      function(waterfallCallback) {
        var db = priv.db();
        var tmpResult = db.view(
          designDoc, 
          viewName, 
          viewOptions, 
          function(err, body) { 
            if (err) {
              var errMsg = 'Using nano.view: error - ' + err;
              log.debug('runView: error - ' + errMsg);
              waterfallCallback(errMsg, []);
            }
            else {
              var docsOrIds = [];
              log.debug('runView: Using nano.view: got response, typeof body - ' + typeof(body) + '.');
              if (_.has(body, 'rows')) {
                log.debug('runView: View matched ' + _.size(body.rows) + ' documents.');
              
                if (_.size(body.rows)) {
                  if ((options.toReturn === 'docs') && viewOptions.include_docs) {
                    docsOrIds = _.pluck(body.rows, "doc");
                    log.debug('runView: Got ' + docsOrIds.length + ' documents...');
                  }
                  else {
                    docsOrIds = _.pluck(body.rows, "id");
                    log.debug('runView: Got ' + docsOrIds.length + ' document ids...');
                  }
                }
              }
              else {
                log.debug('runView: Using nano.view: View returned no rows!');
              }
              waterfallCallback(null, docsOrIds);
            }
          }
        );
      },
      function(docsOrIds, waterfallCallback) {
        if (options.toReturn === 'ids') {
          waterfallCallback(null, docsOrIds);
        }
        else if (options.fetchDocs) {
          var fetchDocsOpts = {
            callback: function(err, docs) {
              waterfallCallback(err, docs);
            }
          };
          if (options.fetchDocsBatchSize) {
            fetchDocsOpts.batchSize = options.fetchDocsBatchSize;
          }
          fetchDocs(docsOrIds,
                    fetchDocsOpts);
        }
        else {
          waterfallCallback(null, docsOrIds);
        }
      }
    ],
    function(err, result) {
      if (err) {
        log.debug('runView: Error processing results, error - ' + err);
        callback && callback(err);
      }
      else {
        if (result.length) {
          log.debug('runView: View returned a result of ' + result.length + ' items.');
        }
        else {
          log.debug('runView: No documents!');
        }
        callback && callback(null, result);
      }
    });
};

/**
 * The main method for saving and processing an image
 */
function save(anImgPath, options, callback)
{
  async.waterfall(
    [
      function(next) {
        parseAndTransform(anImgPath, options, next);
      },

      function(aryPersist, next) {
        persistMultiple(aryPersist, null, next);
      },
      function(aryResult, next) {
        if(options && options.retrieveSavedImage){
          log.debug("After save, retrieving image '%s' by oid '%s'", anImgPath, aryResult[0].oid);
          show(aryResult[0].oid, null,next);
        }else
        {
          next(null,aryResult[0]);
        }
      }
    ],
    function(err, theSavedImage) {
      if (err) {
        var errMsg = util.format("Error occurred while saving image '%s': '%s'", anImgPath, err);
        log.error(errMsg);
        if (_.isFunction(callback)) { callback(errMsg); }
      }
      log.info("Saved image '%s': '%j'", theSavedImage.name, theSavedImage);

      if(options && options.retrieveSavedImage){
        callback(null, theSavedImage);
      }else
      {
        callback(null, {oid:theSavedImage.oid});
      }


    }
  );
}
exports.save = save;


/*
 * When provided with an image path, an optional stream, and a set of transforms,
 * instantiate a gm object, and pass it to parseImage for each variant
 * Invokes the callback with an array of ImageData/Stream to persist
 */
function parseAndTransform(anImgPath, options, callback)
{
  if (!_.isFunction(callback)) throw "parseAndTransform is not very useful if you don't provide a valid callback";

  var saveOriginal = options && _.has(options, 'saveOriginal') ?
    options.saveOriginal : true;

  var variants     = options && _.has(options, 'desiredVariants') && _.isArray(options.desiredVariants) ?
    options.desiredVariants : [];

  var batchId = options && _.isString(options.batch_id) ? options.batch_id : '' ;

  // var variant = variants[0];

  /*
   var imgStream = (_.isObject(anImgStream)) ?
   anImgStream : fs.createReadStream(anImgPath);
   */
  // var imgStream = fs.createReadStream(anImgPath);

  var aryPersist = [];

  // var origOid = '';

  step(
    function() {
      parseImage(anImgPath, this);
    },

    function(err, theImgMeta, theImgPath) {
      if (err) { throw err;}

      theImgMeta.batch_id = batchId;

      if (saveOriginal) {
        aryPersist.push({ data: theImgMeta, stream: theImgPath });
      } else {
        aryPersist.push({ data: theImgMeta });
      }

      if (!_.isObject(variants[0])) {
        // we are done
        log.debug("No variants to process for '%s'", anImgPath);
        callback(null, aryPersist);
      }
      else {

        var iterator = function(variant, next) {
          // variant.orig_id = origOid;
          transform(theImgMeta, variant, function(err, theVariantData, theVariantPath) {
            if (err) { next(err); }
            log.trace('theVariantPath is: %s', theVariantPath);
            aryPersist.push({ data: theVariantData, stream: theVariantPath, isTemp: true });
            next();
          });
        };

        async.forEachSeries( variants, iterator, function(err) {
          if (err) callback(err);
          log.info("Done generating %s variants for image '%s'", variants.length, anImgPath);
          callback(null, aryPersist);
        });
      }
    }
  );
}  // end parseAndTransform


/** returns theImgData, theImgStream */
function transform(anImgMeta, variant, callback)
{
  var gmImg = gm(anImgMeta.path);
  if (log.isDebugEnabled()) { log.debug("Generating variant %j of image '%s'", variant, anImgMeta.path); }
  async.waterfall(
    [
      function(next){
        //TODO: need more validation around variant specs
        if (variant.width || variant.height) {
          var newSize = img_util.fitToSize(anImgMeta.size, { width: variant.width, height: variant.height });
          gmImg.resize(newSize.width, newSize.height);
        }

        var tmp_file_name = config.workDir + '/plm-' + anImgMeta.oid + '-' + variant.name;
        gmImg.write(tmp_file_name, function(err) {
          next(err, tmp_file_name);
        });
      },

      function(aTmpFileName, next){
        parseImage(aTmpFileName, next);
      }
    ],

    // called after waterfall ends
    function(err, theVariantMeta, theVariantPath){
      if (_.has(variant, 'name')) {
        theVariantMeta.name = variant.name;
      }
      theVariantMeta.orig_id  = anImgMeta.oid;
      theVariantMeta.batch_id = anImgMeta.batch_id;
      theVariantMeta.path     = '';

      // timestamp the variants the same as the original, in order to properly sort originals and
      // variants when searching by creation date (this is a couchdb-specific tweak)
      theVariantMeta.created_at = anImgMeta.created_at;
      theVariantMeta.updated_at = anImgMeta.updated_at;

      log.debug("Done processing variant '%s' of image '%s': %j", theVariantMeta.name, anImgMeta.name, theVariantMeta);
      callback(err, theVariantMeta, theVariantPath);
    }
  );
}  // end transform


/*
 * Private method that takes a string path to file system, and:
 *
 * - parses the image,
 * - computes its checksum,
 * - instantiates an Image object, and
 * - invokes callback(err, imgData, imgPath)
 *
 * where:
 *
 * - imgData: an Image object containing the image's metadata
 * - imgPath: a location on the local filesystem where the bits of this file are stored
 *
 * TODO: move this method to an ImageProcessor instance
 * TODO: need a version of this method that returns the gm object so that variants can be generated by
 * re-using the original object
 */
function parseImage(anImgPath, callback)
{
  if (!_.isFunction(callback)) throw "parseImage is not very useful if you don't provide a valid callback";

  var attrs = {path:anImgPath, oid: priv.genOid()};
  if (config.app && _.has(config.app, 'id')) {
    attrs.app_id = config.app.id;
  }
  var imageMeta = mmStorage.docFactory('plm.Image', attrs);
  var gmImg   = gm(fs.createReadStream(anImgPath));

  step(
    function () {
      log.debug("parsing image file '%s'", anImgPath);
      // the 'bufferStream: true' parm is critical, as it buffers the file in memory
      // and makes it possible to stream the bits repeatedly
      gmImg.identify({bufferStream: true},this);
    },

    function (err, data) {
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      log.trace("creating metadata for file '%s'", anImgPath);
      imageMeta.readFromGraphicsMagick(data);
      // log.trace("parsed image: %", util.inspect(imageMeta));
      // this would fail if 'bufferStream' above were not true
      gmImg.stream(this);
    },

    function (err, anImgStream, anErrStream) {
      log.trace("calculating checksum for file '%s'", anImgPath);
      if (config.processingOptions.genCheckSums) {
        cs.gen(anImgStream, this);
      }
      else {
        this();
      }
    },

    function (aString) {
      log.trace("checksum for file '%s' is: %s", anImgPath, aString);
      imageMeta.checksum = aString;
      // log.trace("checksumed image: " + JSON.stringify(imageMeta,null,"  "));
      // gmImg.stream(this);
      callback(null, imageMeta, anImgPath);
    }
  );
}
exports.parseImage = parseImage;

/**
 * Takes an array of persist commands (see 'persist' function description), and invokes them in
 * series, returns an array with the image saved, or the corresponding error
 * TODO: error handling needs to be tested further
 */
function persistMultiple(aryPersist, aryResult, callback)
{
  // handle empty aryPersist
  if ( !(aryPersist instanceof Array) || aryPersist.length === 0)
  {
    var err = 'persistMultiple cowardly refused to persist an empty array of persist instructions';
    log.error(err);
    if (callback instanceof Function) {
      callback(err);
    }
    return;
  }

  // if a results array has not been passed to us, create a new one;
  // passing a result array is helpful for aggregating the results
  // of multiple invocations of this method
  if (!_.isArray(aryResult)) aryResult = [];

  async.forEachSeries(aryPersist, iterator, function(err) {
    if (err) {
      var errMsg = util.format("Error happened while saving image and its variants: %s", err);
      log.err(errMsg);
      callback(errMsg);
    } else {
      callback(null, aryResult);
    }
  });


  function iterator(element, next) {
    persist(element, function(err, image) {
      aryResult.push( err ? err : image );
      next();
    });
  }
}


/*
 * Takes a persist command object of the form:
 *
 *   { data: anImage, stream: aPath }
 *
 * and saves the image record to persistent storage
 *
 * The stream parameter is optional. If passed, the method expects to read the bits of the image at
 * that location, and will save them as an attachment to the image document. Otherwise, only the
 * image's metadata is persisted
 *
 * This should be moved to a DAO class that is couchdb-specific
 */
function persist(persistCommand, callback)
{
  var
    db = nano(
      {
        url: 'http://' + config.db.host + ':' + config.db.port + '/' + config.db.name,
        log: nanoLogFunc
      }
    )
    ,imgData   = persistCommand.data
    ;

  step(

    function () {
      log.trace("saving %j to db...", imgData);
      db.insert(imgData, imgData.oid, this);
    },

    function (err, body, headers) {
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      if (log.isTraceEnabled())log.trace("result from insert: %j", body );

      priv.setCouchRev(imgData, body);

      // log.trace("saved image: %j", imgData);

      if (_.isString(persistCommand.stream)) {
        var attachName = imgData.name;

        if (log.isTraceEnabled()) { log.trace("streaming image bits for file '%s' from path '%s' to storage device", attachName, persistCommand.stream); }
        var imgStream = fs.createReadStream(persistCommand.stream);

        //log.trace("imgData: %j", util.inspect(imgData));
        //log.trace("attachName: %j", attachName);
        //log.trace("stream: %s", util.inspect(imgStream));

        try {
          imgStream.pipe(
            db.attachment.insert(
              imgData.oid,
              attachName,
              null,
              'image/'+imgData.format,
              {rev: imgData._storage.rev}, this)
          );
        }
        catch(e) { log.error("error while streaming: %j", e);}
      } else {
        // we are done
        // TODO this won't work
        callback(null, imgData);
        return;
      }
    },

    function(err, body, headers) {

      // clean-up work directory if this was a temp file generated in the workDir
      if ( persistCommand.isTemp && _.isString(persistCommand.stream) ) {
        fs.unlink(persistCommand.stream, function(err) {
          if (err) { log.warn("error when deleting '%s' from workDir: %j", persistCommand.stream, err); }
        });
      }

      if (err) {
        if (_.isFunction(callback)) callback(err);
        log.error("Error while running persist command %j", persistCommand);
      } else {
        // imgData._storage.rev = body.rev;
        priv.setCouchRev(imgData, body);
        callback(null, imgData);
      }
    }
  );

} // end persist


/**
 * Retrieve an image and its variants by oid; by default, the field Image.metadata_raw is suppressed
 * from the object returned.  If you need this field, pass the showMetadata option.
 *
 * oid must be an oid of an original image, not a variant
 * options:
 *
 *   showMetadata: false by default, set to true to enable display of Image.metadata_raw
 *
 */
function show(oid,options, callback)
{
  var opts = options || {};
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
      log.debug("Retrieving image '%s' and its variants using view '%s'", oid, VIEW_BY_OID_WITH_VARIANT);

      if (log.isTraceEnabled()) {
        if (body) log.trace("body: %s", util.inspect(body));
        if (err)  log.trace("err: %s", util.inspect(err));
      }



      if (!err)
      {
        // if (log.isTraceEnabled()) { log.trace("by_oid_w_variant result: %j", body); }
        if (body.rows.length === 0) {
          log.warn("Unable to find image with oid '%s'", oid);
        } else {
          var docBody = body.rows[0].doc;
          imgOut = mmStorage.docFactory('plm.Image', docBody);
          imgOut.url = priv.getImageUrl(docBody);
          if (opts.showMetadata) { imgOut.exposeRawMetadata = true; }
          if (body.rows.length > 0) {
            for (var i = 1; i < body.rows.length; i++) {
              // log.trace('show: variant - oid - %j, size - %j, orig_id - %j',row.doc.oid, row.doc.geometry, row.doc.orig_id);
              var vDocBody = body.rows[i].doc;
              var vImage = mmStorage.docFactory('plm.Image', vDocBody);
              if (opts.showMetadata) { vImage.exposeRawMetadata = true; }
              vImage.url = priv.getImageUrl(vDocBody);
              // log.trace('show: oid - %j, assigned url - %j',row.doc.oid, vImage.url);
              imgOut.variants.push(vImage);
            }
          }
          // this is logged at DEBUG because it occurs often, and would make the logs verbose
          log.debug("Retrieved image '%s' with oid '%s': %j", imgOut.name, oid, imgOut);
        }

        callback(null, imgOut);

      } else {
        callback("error retrieving image with oid '" + oid + "': " + err);
      }
    }
  );
}
exports.show = show;


/**
 * Main image finder method
 *
 * Retrieve an image and its variants according to the criteria passed in the 'options' object.
 *
 * By default, the field Image.metadata_raw will be suppressed in the objects returned.  If you
 * need this field, pass the showMetadata option.
 *
 * options:
 *  filter: Can take on the following forms:
 *    Object whith a list of rules:
 *      rules: List of rules. Rules may take on the following forms:
 *          field: 'tags'
 *          op: 'eq'
 *          data: <tag value>
 *      groupOp: AND || OR
 *    Single rule, where the rule may be one of:
 *      * filter images w/ tags:
 *        field: 'tags'
 *        op: 'ne'
 *        data: []
 *      * filter images w/o tags:
 *        field: 'tags'
 *        op: 'eq'
 *        data: []
 *        
 *  created:
 *  trashState:
 *  showMetadata: false by default, set to true to enable display of Image.metadata_raw
 */
exports.index = function index(options,callback)
{
  log.debug("Calling 'index' with options: %j", util.inspect(options));

  // TODO:
  //  - The use cases below need to be expanded
  //  - Need to define paging options, and paging impl

  if (!options || _.isEmpty(options) || options.created || (options.filter && !_.has(options.filter, 'rules') && _.has(options.filter, 'data') && (_.size(options.filter.data) === 0))) {
    log.debug('Invoking findByCreationTime...');
    try {
      options = options || {};
      var criteria = options.created || null;
      var opts = {};
      opts.showMetadata = options.showMetadata || false;
      if (options.filter && !_.has(options.filter, 'rules') && _.has(options.filter, 'data') && (_.size(options.filter.data) === 0)) {
        opts.filterRule = options.filter;
      }
      log.debug('findByCreationTime: criteria - ' + util.inspect(criteria) + ', opts - ' + util.inspect(opts) + '...');
      exports.findByCreationTime( criteria, callback, opts);
    }
    catch (e) {
      log.error('findByCreationTime: Error - ' + e);
      callback('find by creation time error - ' + e);
    }
  }
  else {
    if (options.filter) {

      var filterByTag = options.filter;

      exports.findByTags(filterByTag, options,callback);
    }else
    if(options.trashState){
      if(options.trashState ==='in'){
        exports.viewTrash(options,callback);
      }else{
        var trashStateFilter = {};
        trashStateFilter.trashState=options.trashState;
        exports.findImagesByTrashState(trashStateFilter,callback);
      }

    }
  }
};

/**
 * Find images by creation date range. Expects a 'created' array containing a start date and an end
 * date.  A null start date means 'show all from earliest until the end date'.  A null end date means
 * 'show all from start date forward'. Null start and end dates will return all images, so use with
 * caution.
 *
 * options:
 *   filterRule: See exports.index. Single filter rule to filter images w or w/o tags.
 *   showMetadata: false by default, set to true to enable display of Image.metadata_raw
 *   
 */
exports.findByCreationTime = function findByCreationTime( criteria, callback, options )
{
  log.debug("findByCreationTime criteria: %j ", criteria);

  var opts = options || {};

  var view = VIEW_BY_CTIME;
  if (_.has(opts, 'filterRule') && (opts.filterRule.field === 'tags') && (_.size(opts.filterRule.data) === 0)) {
    if (opts.filterRule.op === 'eq') {
      //
      // Filter untagged.
      //
      view = VIEW_BY_CTIME_TAGGED;
    }
    else if (opts.filterRule.op === 'ne') {
      //
      // Filter tagged.
      //
      view = VIEW_BY_CTIME_UNTAGGED;
    }
  }

  log.debug("findByCreationTime opts: " + JSON.stringify(opts));

  var db = priv.db();

  log.debug("findByCreationTime: connected to db...");

  // couchdb specific view options
  var view_opts = {
    startkey: []
    ,include_docs: true
  };

  if (_.isArray(criteria)) {
    view_opts.startkey = priv.date_to_array(criteria[0]);
    view_opts.endkey   = priv.date_to_array(criteria[1]);
  } else if ( _.isString(criteria) ) {
    // TODO handle the case when only a single date is passed
  } else {
    // throw "Invalid Argument Exception: findByCreationTime does not understand options.created argument:: '" + criteria + "'";
  }

  log.trace("Finding images and their variants using view '%s' with view_opts %j", view, view_opts);

  runView(IMG_DESIGN_DOC,
          view,
          {
            toReturn: 'docs',
            fetchDocs: true,
            fetchDocsBatchSize: 100,
            callback: function(err, docs) {
              if (!err) {
                if (docs.length <= 0) {
                  log.warn('findByCreationTime: Unable to find any images.');
                  callback(null, []);
                }
                else {
                  log.debug('findByCreationTime: Retrieved ' + _.size(docs) + ' image documents.');

                  var aryImgOut = convert_couch_body_to_array_of_images(opts,docs);

                  log.debug('findByCreationTime: Returning ' + aryImgOut.length + ' images.');

                  callback(null, aryImgOut);
                }
              }
              else {
                log.error('findByCreationTime: Error retrieving images, error - ' + err);
                callback("error in findByCreationTime with options '" + JSON.stringify(opts) + "': " + err + ".");
              }
            }
          });
}; // end findByCreationTime

/*
* maps the body.rows collection
  into the proper Array of Image originals and their variants
 */
function convert_couch_body_to_array_of_images(opts,resultDocs){

  var aryImgOut = [];
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {};

  for (var i = 0; i < resultDocs.length; i++) {
    var docBody = resultDocs[i];

    anImg = mmStorage.docFactory('plm.Image', docBody);
    if (opts.showMetadata) { anImg.exposeRawMetadata = true; }

    // Assign a URL to the image. Note, this is temporary as the images
    // will eventually move out of Couch / Touch DB.
    anImg.url = priv.getImageUrl(docBody);

    if ( anImg.isOriginal()) {
      log.debug('Adding image to result set, id - ' + anImg.oid);
      imgMap[anImg.oid] = anImg;
      aryImgOut.push(anImg);
    } else {
      // if the image is a variant, add it to the original's variants array
      if (_.isObject(imgMap[anImg.orig_id]))
      {
        if (log.isTraceEnabled()) {
          log.trace('Variant w/ name - %s', anImg.name);
          log.trace('Variant w/ doc. body keys - (%j)', _.keys(docBody));
          log.trace('Variant w/ image keys - (%j)', _.keys(anImg));
        }
        imgMap[anImg.orig_id].variants.push(anImg);
      } else {
        log.warn("Warning: found variant image without a parent %j", anImg);
      }
    }
  }

  return aryImgOut;

}

/**
 * This method converts an array of image docs returned by couch, into an array of images with
 * variants. The couch documents are assumed to be ordered a sequence of original images and their
 * variants:
 *   [Orig, Var, Var..., Orig, Var, Var..., ]
 * it returns:
 *   [Orig, Orig,...] with the Orig.variants field populated
 */
function convertImageViewToCollection(docs, options)
{
  var aryImgOut = [];
  var opts = _.isObject(options) ? options : {};
  var imgMap = {}; // holds a hash of original images that we come across

  for (var i = 0; i < docs.length; i++)
  {
    // log.trace(util.inspect(docs[i]));
    var doc = docs[i].doc;

    if ( doc.class_name === 'plm.Image')
    {
      var anImg = mmStorage.docFactory('plm.Image', doc);
      if (opts.showMetadata) { anImg.exposeRawMetadata = true; }

      // Assign a URL to the image. Note, this is temporary as the images
      // will eventually move out of Couch / Touch DB.
      anImg.url = priv.getImageUrl(doc);

      if ( anImg.isOriginal()) {
        imgMap[anImg.oid] = anImg;
        aryImgOut.push(anImg);
      } else {
        // if the image is a variant, add it to the original's variants array
        if (_.isObject(imgMap[anImg.orig_id])) {
          if (log.isTraceEnabled()) {
            log.trace('Variant w/ name - %s' + anImg.name);
            log.trace('Variant w/ doc. body keys - (%j)', _.keys(doc));
            log.trace('Variant w/ image keys - (%j)', _.keys(anImg));
          }
          imgMap[anImg.orig_id].variants.push(anImg);
        } else {
          log.warn("Warning: found variant image without a parent %j", anImg);
        }
      }
    }
  }
  return aryImgOut;
} // end convertImageViewToCollection


/**
 * Batch imports a collection of images by recursing through a file system
 *
 * options:
 *   - recursionDepth: 0,    // by default performs full recursion, '1' would process only the files inside the target_dir
 *   - ignoreDotFiles: true, // by default ignore .dotfiles
 *   - all options that can be passed to ImageService.save() which will be applied to all images in
 *     the import batch
 *
 * callback is invoked with the initialized importBatch, and processing of the batch will be
 * triggered asynchronously. importBatchShow(oid) can be called to monitor the progress of the
 * importPatch's processing.
 */
function importBatchFs(target_dir, callback, options)
{
  var db = priv.db();
  var importBatch;

  async.waterfall(
    [
      // dive through file system and retrieve array of image paths + mime types
      function(next) {
        collectImagesInDir(target_dir, next);
      },

      // create batchImport record if we have something to import
      function(aryImage, next) {
        if (aryImage.length === 0) {
          log.debug('No images to import.');
          next("No images to import in '" + target_dir + "'");
          return;
        }

        var attrs = { path: target_dir, oid: priv.genOid(), images_to_import: aryImage };

        if (config.app && _.has(config.app, 'id')) {
          attrs.app_id = config.app.id;
        }

        importBatch = mmStorage.docFactory('plm.ImportBatch', attrs);

        if (log.isDebugEnabled()) { log.debug('New importBatch: %j', importBatch); }

        options.batch_id = importBatch.oid;
        log.trace("saving importBatch record to db...");
        log.debug('Saving batch, name - ' + importBatch.oid + ', rev - ' + importBatch._storage.rev);
        db.insert(importBatch, importBatch.oid, next);
      },

      function (body, headers, next) {
        priv.setCouchRev(importBatch, body);
        log.debug("Saved importBatch record to db before processing:  id '%s' -  rev '%s'", importBatch.oid, importBatch._storage.rev);

        priv.markBatchInit(importBatch);

        // return the initialized importBatch...
        if (_.isFunction(callback)) {
          callback(null, importBatch);
        }

        log.info("Starting importBatch processing of path '%s': %j", importBatch.path, importBatch);

        // and continue processing asynchronously
        saveBatch(importBatch, options, next);
      }

    ],
    function(err) {
      // priv.markBatchComplete(importBatch);
      if (err) {
        if (importBatch) {
          var errMsg = util.format("Error while processing importBatchFs '%s': %s", importBatch.oid,err);
        }
      }
    }
  );
}
exports.importBatchFs = importBatchFs;


/**
 * Saves all the images specified in the ImportBatch, and
 * collects saved images or errors, as the case may be
 *
 * options:
 *   - retrieveBatchOnSave: false by default,
 *     if true, performs an importBatchShow(oid) after saving the batch
 */
function saveBatch(importBatch, options, callback)
{
  var db = priv.db();
  var opts = options || {};

  if (!_.has(opts, "retrieveBatchOnSave")) {
    opts.retrieveBatchOnSave = false;
  }

  importBatch.setStartedAt(new Date());
  log.debug("Saving images in importBatch '%s'", importBatch.path);

  // The literal integer in the function call below limits the number of concurrent
  // processes that are spawned to save the import batch.  There is a noticeable increase in
  // throughput when increasing this number from 1 to 3.  Beyond that, the load of the system
  // increases substantially without a further increase in throughput. Mileage may vary on your
  // system.
  var numJobs = 1;

  if (config.processingOptions && config.processingOptions.numJobs) {
    numJobs = config.processingOptions.numJobs;
  }

  async.forEachLimit(importBatch.images_to_import, numJobs, saveBatchImage, function(err) {
    // we are done processing each image in the batch
    priv.markBatchComplete(importBatch);

    log.debug('Saving batch, name - ' + importBatch.oid + ', rev - ' + importBatch._storage.rev);

    db.insert(importBatch, importBatch.oid, function(err, body, headers) {
      if (err) {
        log.error("Error while saving importBatch '%s': %s", importBatch.path, err);
      } else {
        if (log.isTraceEnabled()) { log.trace("result of importBatch save: %j", body); }
        priv.setCouchRev(importBatch, body); // just in case
        log.info("Successfully saved importBatch '%s': %j", importBatch.path, importBatch);
      }
      if (opts.retrieveBatchOnSave) {
        importBatchShow(importBatch.oid, null, callback);
      }
    });
  });

  /** pathSpec will be of the form: {path: 'somePath', format: 'jpg'} */
  function saveBatchImage(pathSpec, next) {
    var imgPath = pathSpec.path;
    save(
      imgPath
      ,options
      ,function(err, image) {
        if (err) {
          log.error("error while saving image at '%s': %s", imgPath, err);
          importBatch.addErr(imgPath, err);
        } else {
          importBatch.addSuccess(image);
        }
        next();
      }
    );
  }
}


/**
 * Retrieves an importBatch by oid
 *
 * options:
 *   includeImages:
 *     true by default, if true returns all images with variants that are part of the batch
 *     if false, returns only the batchImport metadata
 *
 */
function importBatchShow(oid, options, callback) {

  var batchOut = {};

  if (_.isObject(priv.batch_in_process[oid]))
  {
    batchOut = priv.batch_in_process[oid];
    log.info("Retrieved importBatch with oid '%s' from in-process cache: %j", oid, batchOut);
    callback(null, batchOut);
    return;
  }

  var db = priv.db();
  var opts = _.isObject(options) ? options : {};

  if (!_.has(opts,'includeImages')) { opts.includeImages = true; }

  var view = VIEW_BATCH_BY_OID_W_IMAGE;

  // view keys have the format:
  // batchOid,
  // imageOid (or "0" for a batch record)
  // 0 = batch, 1 = original, 2 = variant
  var view_opts = {
    //startkey: [oid,null,0,0]
    //,endkey:  [oid,{},2,0]
    startkey: [oid,null]
    ,endkey:  [oid,{}]
    ,include_docs: true
  };

  log.debug("retrieving importBatch with oid '%s' using view '%s' with view_opts %j ...", oid, view, view_opts);

  db.view(IMG_DESIGN_DOC, view, view_opts,
    function(err, body) {
      if (err) {
        var errMsg = util.format("error in importBatchShow(%s): %j", oid, err);
        log.error(errMsg);
        callback(errMsg);
      }
      else
      {
        log.trace("view %s returned %s rows: %j", view, body.rows.length,body);

        if (body.rows.length <= 0) {
          log.warn("Could not find an importBatch with oid '%s'", oid);
        } else {
          var doc = body.rows[0].doc;
          if (doc.class_name !== 'plm.ImportBatch') {
            throw util.format("Invalid view returned in importBatchShow by view '%s'", view);
          }

          batchOut = mmStorage.docFactory('plm.ImportBatch', doc);


          if (opts.includeImages) {
            //by default include images out of trash
            var imagesTrashState = "out";

            if(opts.imagesTrashState){
              imagesTrashState = opts.imagesTrashState;
            }

            var images = convertImageViewToCollection(body.rows);
            var filteredImages = [];
            //filter images by trash state
            if(imagesTrashState==="out"){
              for (var i = 0; i < images.length; i++) {
                if(images[i].inTrash()===false){
                  filteredImages.push(images[i]);
                }
              }

            }else if(imagesTrashState==="in"){
              for (var i = 0; i < images.length; i++) {
                if(images[i].inTrash()===true){
                  filteredImages.push(images[i]);
                }
              }

            }else if(imagesTrashState==="any"){// any (all images of importbatch)
              filteredImages=images;
            }
            batchOut.images = filteredImages;

            log.info("Retrieved importBatch with oid '%s' and %s images: %j", oid, batchOut.images.length, batchOut);
          } else {
            log.info("Retrieved importBatch with oid '%s' and includeImages = false: %j", oid, batchOut);
          }
        }
        callback(null, batchOut);
      }
    }
  );
}
exports.importBatchShow = importBatchShow;


/**
 * Lists the 'N' most recent import batches
 *
 * N: Number of batches to return. If undefined, all batches are returned.
 * options:
 *   includeImages: false by default, if true returns all images with variants that are part of the batch
 */
function importBatchFindRecent(N, options, callback) {
  var db = priv.db();
  var aryBatchOut = []; //
  var opts = _.isObject(options) ? options : {};
  var view = VIEW_BATCH_BY_CTIME;

  var view_opts = {
    descending: true
    ,include_docs: true
  };

  if (N !== undefined) {
    view_opts.limit = N;
  }

  log.debug("Finding %s most recent importBatches with options %j", N, options);
  log.trace("Finding %s most recent importBatches using view '%s' with view_opts %j", N, view, view_opts);

  db.view(IMG_DESIGN_DOC, view, view_opts,
    function(err, body) {
      if (err) {
        var errMsg = util.format("error in importBatchFindRecent: %j", err);
        log.error(errMsg);
        callback(errMsg);
      } else {
        log.trace("view %s returned %s rows: %j", view, body.rows.length, body);
        for (var i = 0; i < body.rows.length; i++) {
          var doc = body.rows[i].doc;
          if (doc.class_name === 'plm.ImportBatch') {
            var importBatch = mmStorage.docFactory('plm.ImportBatch', doc);
            priv.setCouchRev(importBatch, doc);
            aryBatchOut.push(importBatch);
          }
        } // end for

        var success = util.format("Successfully retrieved '%s' most recent batch imports: %j", N, aryBatchOut);

        if (opts.includeImages) {
          importBatchRetrieveImages(aryBatchOut, options, function(err, out) {
            if (!err) {log.debug(success);}
            callback(err,out);
          });
        } else {
          log.debug(success);
          callback(null, aryBatchOut);
        }
      }
    }
  );

}
exports.importBatchFindRecent = importBatchFindRecent;


/**
 * Given an array of import batches, retrieves the images and variants imported during that batch,
 * and adds them to ImportBatch.images
 */
function importBatchRetrieveImages(aryBatch, options, callback)
{
  var aryBatchOut = [];
  log.debug("Populating images for collection of batches: %j", aryBatch);

  async.forEachLimit( aryBatch, 3, iterator,function(err) {
    if (err) {
      var msgErr = util.format("Error while retrieving images for importBatch: %j", err);
      log.error(msgErr);
      callback(err);
    } else {
      log.debug("Done loading importBatches with images");
      callback(null, aryBatchOut);
    }
  });

  function iterator(batch, next) {
    importBatchShow(batch.oid, options, function(err, theBatch) {
      if (err) { next(err); }
      else {
        aryBatchOut.push(theBatch);
        next();
      }
    });
  }
}


/**
 * Recurses inside a folder and returns a tuple
 *   {path: "target_dir/someImage", format: "jpg"}
 * for all the images that it finds underneath the folder;
 * TODO: add a parameter to limit the recursion level
 */
function collectImagesInDir(target_dir, callback)
{
  var aryFile  = [];
  var aryImage = [];

  // This inner function is called further below on each file under the target directory
  // to determine whether it is an image and to determine its mime type
  function collectImage(file, next) {
    // log.trace("collecting %s", file);
    mime(file, function(err, mimeType) {
      if (err) {
        log.warn("error while collecting images: %s", err);
        next(err);
        return;
      }

      // converts a string like 'image/jpg' to ['image', 'jpg']
      var mimeData = mimeType.split("/");

      if (mimeData[0] === 'image') {
        aryImage.push({ path: file, format: mimeData[1] });
      }
      next();
    });
  }


  // start collecting...
  async.waterfall(
    [
      // collect all files inside the target directory first
      function(next) {
        log.trace("Diving into %s", target_dir);
        dive(target_dir, {directories: false},
          function(err, file) {
            if (err) { log.warn("error while diving through '%s': %s", target_dir, err); }
            else {aryFile.push(file);}
          },
          function() { next();}
        );
      },

      function(next) {
        log.debug("found %s files total under '%s'", aryFile.length, target_dir);

        // calling 'mime' concurrently can lead to a 'too many open files' error
        // so we limit the number of concurrent calls to mime, with the forEachLimit below.
        // In addition, raising the limit leads to higher system loads,
        // with no apparent improvement in performance.
        async.forEachLimit(aryFile, 3, collectImage, next);
      }
    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while collecting images in directory '%s': %s", target_dir, err);
        callback(err);
      } else {
        log.info("Found %s images under directory '%s'", aryImage.length, target_dir);
        callback(null, aryImage);
      }
    }
  );
} // end collectImagesInDir
exports.collectImagesInDir = collectImagesInDir;



exports.saveOrUpdate = saveOrUpdate;

/**
 * Creates an image in database if the image does not exist, updates the image otherwise
 *
 * @param theDoc
 * @param tried
 * @param callback
 */
function saveOrUpdate(options, callback) {

  var db = priv.db();

  var theDoc = options.doc,
    tried = options.tried;

  if(!strUtils.isBlank(theDoc.oid)){
    theDoc.updated_at = new Date();
  }

  db.insert(toCouch(theDoc), theDoc.oid, function (err, http_body, http_header) {

    if (err) {

      if (err.error === 'conflict' && tried < 1) {

        // get record _rev and retry
        return db.get(theDoc.oid, function (err, doc) {

          theDoc._rev = doc._rev;
          saveOrUpdate({doc:theDoc, "tried":tried + 1},callback);

        });

      }else{
        callback(err);
      }

    }
    else{
      if (_.isFunction(callback)) {
        callback(null, http_body);

      }
    }

  });
}

exports.toCouch = toCouch;

/**
 *
 * @param image
 * @return image object with added Couch specific attributes like the:
 *  _rev (revision)
 *  _attachments
 */
function toCouch(image){
  var out = image.toJSON();
  out._rev = image._rev;

  out._attachments = image._attachments;

  delete (out.url);
  return out;
}



/**
 * Find images by tags. Expects a filter object of the form:
 *
 var filterByTag = {
    "groupOp":"AND",    // OR is also allowed
    "rules":[
      {
        "field":"tags",
        "op":"eq",
        "data":"friends"
      },
      {
        "field":"tags",
        "op":"eq",
        "data":"family"
      }
    ]
  };
 *
 * options:
 *
 *   showMetadata: false by default, set to true to enable display of Image.metadata_raw
 */
exports.findByTags = findByTags;


function findByTags(filter, options, callback) {
  log.debug("findByTags filter: %j ", filter);

  var opts = options || {};

  log.debug("findByTags opts: " + util.inspect(opts));

  var db = priv.db();

  log.trace("findByTags: connected to db...");

  var aryImgOut = []; // images sorted by creation time
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {};

  // couchdb specific view options
  var view_opts={ include_docs: true, reduce:false};

  if (_.isObject(filter)) {
    view_opts.keys = _.pluck(filter.rules, 'data');
  }else {
    throw "Invalid Argument Exception: findByTags does not understand filter argument:: '" + filter + "'";
  }


  log.trace("Finding images and their variants using view '%s' with view_opts %j", VIEW_BY_TAG, view_opts);

  var tags = view_opts.keys;

  db.view(IMG_DESIGN_DOC, VIEW_BY_TAG, view_opts,
    function(err, body) {

      if (!err) {

        //remove duplicates
        var possibleMatches = _.uniq(body.rows,function (doc) {
          return doc.id;
        });

        // Pick out the documents that include all of the given keywords.

        var results = _.filter(possibleMatches, function(m) {
            if(filter.groupOp==="AND"){
              var containsAll = _.every(tags,  function(tag){
                return _.contains(m.doc.tags, tag);
              });
              return containsAll;

            }else
            if(filter.groupOp==="OR"){
              var containsAny = _.some(tags,  function(tag){
                return _.contains(m.doc.tags, tag);
              });
              return containsAny;
            }
          }
        );

        //extract only the "doc" part
        var resultDocs = _.pluck(results, "doc");

        var resultDocsOids = _.pluck(resultDocs, "oid");


        var aryImgOut = [];

         async.forEachLimit(resultDocsOids,
          1,
          function (oid,next){
            show(oid,null,function(err,image){
              if (err) { callback(err); }
              else {
                aryImgOut.push(image);
              }
              next();
              }
            );
           },
          function(err) {
            if (err) {
              callback(err);
            }else{
              //all images were processed time to callback
              callback(null, aryImgOut);
            }
        }
        );


      } else {
        callback(util.format("error in findByTags with view options '%j' - err: %s - body: %j", view_opts, err, body));
      }
    }
  );
} // end findByTags


/**
 * options.trashState : in|out|any
 * @type {Function}
 */
exports.findImagesByTrashState = findImagesByTrashState;


function findImagesByTrashState(options, callback) {

  var opts = options || {};

  log.debug("findImagesByTrashState opts: " + util.inspect(opts));

  var db = priv.db();

  log.trace("findImagesByTrashState: connected to db...");

  var aryImgOut = []; // images sorted by creation time
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {};

  // couchdb specific view options
  var view_opts={ include_docs: true};


  log.trace("Finding images by trashState using view '%s' with view_opts %j", VIEW_BY_OID_WITHOUT_VARIANT, view_opts);


  db.view(IMG_DESIGN_DOC, VIEW_BY_OID_WITHOUT_VARIANT, view_opts,
    function(err, body) {

      if (!err) {

        //remove duplicates
        var possibleMatches = _.uniq(body.rows,function (doc) {
          return doc.id;
        });

        // Filter images by trashState
        var results = _.filter(possibleMatches, function(m) {
              if(opts.trashState==="in"){
                  return m.doc.in_trash===true;
              }else
              if(opts.trashState==="out"){
                return !(m.doc.in_trash); //m.doc.in_trash is undefined null or false
              }else
              if(opts.trashState==="any"){
                return true; //return the doc regardless of it trashState
              }
            }
        );

        //extract only the "doc" part
        var resultDocs = _.pluck(results, "doc");

        var resultDocsOids = _.pluck(resultDocs, "oid");


        var aryImgOut = [];

        async.forEachLimit(resultDocsOids,
          1,
          function (oid,next){
            show(oid,null,function(err,image){
                if (err) { callback(err); }
                else {
                  aryImgOut.push(image);
                }
                next();
              }
            );
          },
          function(err) {
            if (err) {
              callback(err);
            }else{
              //all images were processed time to callback
              callback(null, aryImgOut);
            }
          }
        );


      } else {
        callback(util.format("error in findImagesByTrashState with view options '%j' - err: %s - body: %j", view_opts, err, body));
      }
    }
  );
} // end findByTrashState


exports.findByOids = findByOids;


function findByOids(oidsArray, options, callback) {
  log.debug("findByOids array of oids: %j ", oidsArray);

  var opts = options || {};

  log.debug("findByOids opts: " + JSON.stringify(opts));

  var db = priv.db();

  log.debug("findByOids: connected to db...");

  var aryImgOut = []; // images sorted by creation time
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {};

  // couchdb specific view options
  var view_opts={include_docs: true};

  if (_.isArray(oidsArray)) {
    view_opts.keys = oidsArray;
  }else {
    throw "Invalid Argument Exception: findByOids does not understand filter oidsArray:: '" + oidsArray + "'";
  }


  log.trace("Finding images and their variants using view '%s' with view_opts %j", VIEW_BY_OID_WITHOUT_VARIANT, view_opts);

  var tags = view_opts.keys;

  db.view(IMG_DESIGN_DOC, VIEW_BY_OID_WITHOUT_VARIANT, view_opts,
    function(err, body) {

      if (!err) {

        //remove duplicates
        var results = _.uniq(body.rows,function (doc) {
          return doc.id;
        });


        //extract only the "doc" part
        var resultDocs = _.pluck(results, "doc");

        var aryImgOut = convert_couch_body_to_array_of_images(opts,resultDocs);

        callback(null, aryImgOut);

      } else {
        callback("error in findByTags with options '" + options + "': " + err + ", with body '" + JSON.stringify(body) + "'");
      }
    }
  );
} // end findByOids


exports.tagsReplace = tagsReplace;

/**
 * The tags in oldTags will be replaced by the tags in newTags
 * oldTags[1] will be replaced by newTags[1]
 * oldTags[2] will be replaced by newTags[2]
 *          .
 *          .
 * oldTags[n] will be replaced by newTags[n]
 *
 * @param oidArray
 * @param oldTags
 * @param newTags
 * @param callback
 */
function tagsReplace(oidArray,oldTags, newTags,callback){

  var imagesToModify = null;

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Finding by oids ...");

        findByOids(oidArray, null, function (err, images) {
          if (err) {
            var errMsg = util.format("Error occurred while finding by oids ", err);
            log.error(errMsg);
            if (_.isFunction(callback)) { callback(errMsg); }
          }
          else{
            imagesToModify = images;
            next();
          }
        });

      },

      //replace the tags
      function(next) {
        for (var i = 0; i < imagesToModify.length; i++) {
          imagesToModify[i].tagsReplace(oldTags,newTags);
        }
        next();
      },

      //save the modified images
      function(next) {
        var saveOrUpdateParameters = _.map(imagesToModify, function(image){ return {"doc":image,"tried":0}; });
        async.forEachLimit(saveOrUpdateParameters, 3, saveOrUpdate, function(err) {

          if (err) {
            log.error("Error while updating images tags", err);
            next(err);
          } else {
            log.info("Successfully updated images tags");
          }
          next();

        });

      }

    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while replacing tags on images ", err);
        callback(err);
      } else {
        log.info("Successfully replaced tags on images");
        callback(null);
      }
    }
  );



} // end tagsReplace

exports.tagsRemove = tagsRemove;

/**
 * @param oidArray
 * @param tagsToRemove
 * @param callback
 */
function tagsRemove(oidArray,tagsToRemove,callback){

  var imagesToModify = null;

  if (!oidArray) {
    callback("No images provided on attempt remove tags.");
    return;
  }
  else if (!tagsToRemove) {
    callback("No tags provided on attempt to remove tags from images - " + oidArray.toString());
    return;
  }

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Finding by oids ...");

        findByOids(oidArray, null, function (err, images) {
          if (err) {
            var errMsg = util.format("Error occurred while finding by oids ", err);
            log.error(errMsg);
            if (_.isFunction(callback)) { callback(errMsg); }
          }
          else{
            imagesToModify = images;
            next();
          }
        });

      },

      //remove the tags
      function(next) {
        for (var i = 0; i < imagesToModify.length; i++) {
          imagesToModify[i].tagsDelete(tagsToRemove);
        }
        next();
      },

      //save the modified images
      function(next) {
        var saveOrUpdateParameters = _.map(imagesToModify, function(image){ return {"doc":image,"tried":0}; });
        async.forEachLimit(saveOrUpdateParameters, 3, saveOrUpdate, function(err) {

          if (err) {
            log.error("Error while updating images tags", err);
            next(err);
          } else {
            log.info("Successfully updated images tags");
          }
          next();

        });

      }

    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while removing tags on images ", err);
        callback(err);
      } else {
        log.info("Successfully removing tags on images");
        callback(null);
      }
    }
  );



} // end tagsRemove

exports.tagsAdd = tagsAdd;
/**
 * Add a list of tags to a each image in a list of images.
 * @param oidArray
 * @param tagsArray
 * @param callback
 */
function tagsAdd(oidArray, tagsArray,callback){

  var imagesToModify = null;

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Finding by oids ...");

        findByOids(oidArray, null, function (err, images) {
          if (err) {
            var errMsg = util.format("Error occurred while finding by oids ", err);
            log.error(errMsg);
            if (_.isFunction(callback)) { callback(errMsg); }
          }
          else{
            imagesToModify = images;
            next();
          }
        });

      },

      //add the tags
      function(next) {
        for (var i = 0; i < imagesToModify.length; i++) {
          imagesToModify[i].tagsAdd(tagsArray);
        }
        next();
      },

      //save the modified images
      function(next) {
        var saveOrUpdateParameters = _.map(imagesToModify, function(image){ return {"doc":image,"tried":0}; });
        async.forEachLimit(saveOrUpdateParameters, 3, saveOrUpdate, function(err) {

          if (err) {
            log.error("Error while adding images tags", err);
            next(err);
          } else {
            log.info("Successfully added images tags");
          }
          next();

        });

      }

    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while adding tags on images ", err);
        callback(err);
      } else {
        log.info("Successfully added tags on images");
        callback(null,oidArray);
      }
    }
  );



} // end tagsAdd



exports.tagsGetAll = tagsGetAll;
/**
 * Get the list of all the tags in the database
 * @param callback
 */
function tagsGetAll(callback){

  log.debug("Attempting to get all tags in database .........");

  var db = priv.db();

  log.trace("tagsGetAll: connected to db...");


  // couchdb specific view options
  var view_opts={ include_docs: false
    ,reduce:true
    ,group:true};


  log.trace("Getting all tags using view '%s' with view_opts %j", VIEW_BY_TAG, view_opts);

  db.view(IMG_DESIGN_DOC, VIEW_BY_TAG, view_opts,
    function(err, body) {

      if (!err) {

        var tags = _.pluck(body.rows, "key");
        log.debug("getAllTags query returned: '%j'", tags);

        callback(null, tags);

      } else {
        callback(util.format("error getting all tags: '%s', with body: '%j'", err, body));
      }
    }
  );


} // end tagsGetAll



exports.tagsGetImagesTags = tagsGetImagesTags;
/**
 * Get the list of tags of a set of images
 * @param callback
 */
function tagsGetImagesTags(imagesIdsArray,callback){

  log.debug("Attempting to get tags of a set of images .........");

  var db = priv.db();

  log.trace("tagsGetImagesTags: connected to db...");


  // couchdb specific view options
  var view_opts={ include_docs: true
  };
  if (_.isArray(imagesIdsArray)) {
    view_opts.keys = imagesIdsArray;
  }else {
    throw "Invalid Argument Exception: tagsGetImagesTags does not understand imagesIdsArray:: '" + imagesIdsArray + "'";
  }

  log.trace("Getting tags of a set of images using view '%s' with view_opts %j", VIEW_BY_OID_WITHOUT_VARIANT, view_opts);

  db.view(IMG_DESIGN_DOC, VIEW_BY_OID_WITHOUT_VARIANT, view_opts,
    function(err, body) {

      if (!err) {

        //extract only the "doc" part
        var resultDocs = _.pluck(body.rows, "doc");

        var resultTags = [];

        //collect all the tags
        _.each(resultDocs,function(image){
          resultTags.push(image.tags);
        });

        resultTags = _.flatten(resultTags);

        //remove duplicates
        resultTags = _.uniq(resultTags);

        //sort tags
        resultTags.sort();


        log.debug("tagsGetImagesTags query returned: '%j'", resultTags);

        callback(null, resultTags);

      } else {
        callback(util.format("error getting tags of a set of images: '%s', with body: '%j'", err, body));
      }
    }
  );


} // end tagsGetImagesTags



/*
 *  getImageUrl: Helper to construct a URL to reference the image associated with a document.
 */
priv.getImageUrl = function(doc) {
  var url = 'http://' + config.db.host;
  if (config.db.port) {
    url = url + ':' + config.db.port;
  }
  url = url + '/';
  if (config.db.name) {
    url = url + config.db.name + '/';
  }
  else {
    return null;
  }
  if (doc._id) {
    url = url + doc._id + '/';
  }
  else {
    return null;
  }
  if (_.has(doc, '_attachments')) {
    if (_.has(doc, 'orig_id') && (doc.orig_id !== '')) {
      url = (_.has(doc, 'name') && _.has(doc._attachments, doc.name))? url + doc.name : null;
    }
    else {
      url = _.keys(doc._attachments)? url + _.first(_.keys(doc._attachments)) : null;
    }
    return url;
  }
  else {
    return null;
  }
};


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
 */
priv.date_to_array = function date_to_array(aDate)
{
  if (_.isString(aDate)) {
    aDate = moment(aDate, 'YYYYMMDD').toDate();
  }

  if (!_.isDate(aDate)) {
    throw "Invalid Argument Exception: argument is not a Date, or unable to parse string argument into a Date";
  }

  return [
    aDate.getFullYear()
    ,aDate.getMonth()+1
    ,aDate.getDate()
    ,aDate.getHours()
    ,aDate.getMinutes()
    ,aDate.getSeconds()
    ,aDate.getMilliseconds()
  ];
};

/** populates the '_storage' field of persistent entity */
priv.setCouchRev = function setCouchRev(entity, couch_result) {
  entity._storage.type = 'couchdb';
  entity._storage.id  = couch_result.id;
  entity._storage.rev = couch_result.rev;
};

exports.sendToTrash = sendToTrash;

/**
 * TODO: Move this method to a StorageService
 *
 * @param oidArray
 * @param callback
 */
function sendToTrash(oidArray,callback){

  var imagesToModify = [];

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Finding images to send to Trash ...");

        function iterator(imageOid, next2) {
          show(imageOid, null, function(err, image) {
            if (err) { next2(err); }
            else {
              imagesToModify.push(image);
              //add also the variants
              if(_.isArray(image.variants) ){
                for (var i = 0; i < image.variants.length; i++) {
                  imagesToModify.push(image.variants[i]);
                }
              }
              next2();
            }
          });
        }

        async.forEachLimit( oidArray, 3, iterator,function(err) {
          if (err) {
            var msgErr = util.format("Error finding images to send to trash: %j", err);
            log.error(msgErr);
            next(err);
          } else {
            log.debug("Done finding images to send to trash");
            next();
          }
        });

      },

      //send image to trash
      function(next) {
        for (var i = 0; i < imagesToModify.length; i++) {
          imagesToModify[i].sendToTrash();
        }
        next();
      },

      //save the modified images
      function(next) {
        var saveOrUpdateParameters = _.map(imagesToModify, function(image){ return {"doc":image,"tried":0}; });
        async.forEachLimit(saveOrUpdateParameters, 3, saveOrUpdate, function(err) {

          if (err) {
            log.error("Error while sending images to trash", err);
            next(err);
          } else {
            log.info("Successfully sent images to trash");
          }
          next();

        });

      }

    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while sending images to trash ", err);
        callback(err);
      } else {
        log.info("Successfully sent images to trash");
        var theSentToTrashImages = imagesToModify;
        callback(null,theSentToTrashImages);
      }
    }
  );

} // end sendToTrash


exports.restoreFromTrash = restoreFromTrash;

/**
 * TODO: Move this method to a StorageService
 *
 * @param oidArray
 * @param callback
 */
function restoreFromTrash(oidArray,callback){

  var imagesToModify = [];

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Finding images to restore from Trash ...");

        function iterator(imageOid, next2) {
          show(imageOid, null, function(err, image) {
            if (err) { next2(err); }
            else {
              imagesToModify.push(image);
              //add also the variants
              if(_.isArray(image.variants) ){
                for (var i = 0; i < image.variants.length; i++) {
                  imagesToModify.push(image.variants[i]);
                }
              }
              next2();
            }
          });
        }

        async.forEachLimit( oidArray, 3, iterator,function(err) {
          if (err) {
            var msgErr = util.format("Error finding images to restore from trash: %j", err);
            log.error(msgErr);
            next(err);
          } else {
            log.debug("Done finding images to restore from trash");
            next();
          }
        });

      },

      //restore from trash
      function(next) {
        for (var i = 0; i < imagesToModify.length; i++) {
          imagesToModify[i].restoreFromTrash();
        }
        next();
      },

      //save the modified images
      function(next) {
        var saveOrUpdateParameters = _.map(imagesToModify, function(image){ return {"doc":image,"tried":0}; });
        async.forEachLimit(saveOrUpdateParameters, 3, saveOrUpdate, function(err) {

          if (err) {
            log.error("Error while restoring images from trash", err);
            next(err);
          } else {
            log.info("Successfully restored images from trash");
          }
          next();

        });

      }

    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while restoring images from trash ", err);
        callback(err);
      } else {
        log.info("Successfully restored images from trash");
        var theImagesRestoredFromTrash = imagesToModify;
        callback(null,theImagesRestoredFromTrash);
      }
    }
  );

} // end restoreFromTrash

exports.viewTrash = viewTrash;

/**
 * TODO: Move this method to a StorageService
 *
 * Find documents in Trash
 *
 * options:
 *
 *   showMetadata: false by default, set to true to enable display of Image.metadata_raw
 */


function viewTrash(options, callback) {

  var opts = options || {};

  var db = priv.db();

  log.trace("viewTrash: connected to db...");

  var aryImgOut = [];
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {};

  // couchdb specific view options
  var view_opts={ include_docs: true};


  db.view(IMG_DESIGN_DOC, VIEW_TRASH, view_opts,
    function(err, body) {

      if (!err) {

        //remove duplicates
        var results = _.uniq(body.rows,function (doc) {
          return doc.id;
        });

        //extract only the "doc" part
        var resultDocs = _.pluck(results, "doc");


        var aryImgOut = [];

        async.forEachLimit(resultDocs,
          1,
          function (doc,next){
            var anImage = mmStorage.docFactory('plm.Image', doc);
            if(anImage.isOriginal()){//retrieve also the variants
              show(doc.oid,null,function(err,image){
                  if (err) { callback(err); }
                  else {
                    aryImgOut.push(image);
                  }
                  next();
                }
            )
            }
            else{
              //is a variant, do nothing since the variant is already nested in the variants attribute of an original
              next();
            }
          }
          ,
          function(err) {
            if (err) {
              callback(err);
            }else{
              //all images were processed time to callback
              callback(null, aryImgOut);
            }
          }
        );

      } else {
        callback(util.format("error in viewTrash with view options '%j' - err: %s - body: %j", view_opts, err, body));
      }
    }
  );
} // end viewTrash



exports.deleteImages = deleteImages;
/**
 * TODO: Move this method to a StorageService
 *
 * This method destroys the images permanently
 * @param callback
 */
function deleteImages(oidArray, callback){

  var db = priv.db();

  var imagesToDelete = [];
  var imagesToDeletePermanently = {docs:[]};

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Finding images to delete permanently ...");

        function iterator(imageOid, next2) {
          show(imageOid, null, function(err, image) {
            if (err) { next2(err); }
            else {
              imagesToDelete.push(image);
              //add also the variants
              if(_.isArray(image.variants) ){
                for (var i = 0; i < image.variants.length; i++) {
                  imagesToDelete.push(image.variants[i]);
                }
              }
              next2();
            }
          });
        }

        async.forEachLimit( oidArray, 3, iterator,function(err) {
          if (err) {
            var msgErr = util.format("Error finding images to delete permanently: %j", err);
            log.error(msgErr);
            next(err);
          } else {
            log.debug("Done finding images to delete permanently");
            next();
          }
        });

      },

      function(next) {
        for (var i = 0; i < imagesToDelete.length; i++) {

          //To delete a document set the _deleted member to true
          var docToDestroy = {};
          docToDestroy._id = imagesToDelete[i]._id;
          docToDestroy._rev = imagesToDelete[i]._rev;
          docToDestroy._deleted = true;

          imagesToDeletePermanently.docs.push(docToDestroy);

        }

        next();
      },
      function(next) {
        db.bulk(imagesToDeletePermanently, next);
      }
    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while deleting images permanently", err);
        callback(err);
      } else {
        log.info("Successfully deleted images permanently");
        callback(null);
      }
    }
  );

} // end deleteImages


exports.emptyTrash = emptyTrash;

/**
 * TODO: Move this method to a StorageService
 *
 * This method destroys the images permanently
 * @param callback
 */
function emptyTrash(callback){

  var db = priv.db();

  var imagesToDelete = {docs:[]};

  async.waterfall(
    [
      //Retrieve the images to modify
      function(next) {

        log.trace("Attempting to empty trash ...");

        viewTrash(null, function (err, docs) {
          if (err) {
            callback(err);
          } else {

            var oidsToDelete = _.pluck(docs, "oid");

            deleteImages(oidsToDelete,next);

          }
        });

      }
    ],

    // called after waterfall completes
    function(err) {
      if (err) {
        log.error("Error while emptying trash", err);
        callback(err);
      } else {
        log.info("Successfully emptied trash");
        callback(null);
      }
    }
  );

} // end emptyTrash

// export all functions in pub map
/*
 for (var func in pub) {
 if(_.isFunction(pub[func])) {
 exports[func] = pub[func];
 }
 }
 */
