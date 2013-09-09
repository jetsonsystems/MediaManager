//
// ImageService: Interface to creating / reading / updating image documents. Includes processing of
//  images to get meta-data, and generate required thumbnails.
//
//  Public methods include:
//
//    findVersion(oid, callback): Determines whether an image w/ oid exists.
//    create(imgAttrs, options, callback): Creates new images give a set of initial attributes (imgAttrs).
//      No image processing (parsing and/or thumbnailing) is performed.
//    index(options, callback): Retrieves a collection of images.
//    save(anImgPath, options, callback): Creates a new image, parsing via GraphicsMagick, 
//      and optionally generating thumbnails. The resulting imagke document is persisted.
//    saveOrUpdate(options, callback): Creates or updates an image. No processing is performed.
//    show(oid, options, callback): Retreives and returns an image.
//    toCouch(image): ?
//    findByCreationTime(criteria, callback, options): Retrieves given 'criteria' and returns sorted
//      by creation time.
//    findByTags(filter, options, callback): Retrieve images by tag(s).
//    findImagesByTrashState(options, callback): Retrieve images which are in or not in trash.
//    findByOids(oidsArray, options, callback): ?
//    tagsReplace(oidArray, oldTags, newTags, callback):
//    tagsRemove(oidArray, tagsToRemove, callback):
//    tagsAdd(oidArray, tagsArray, callback):
//    tagsGetAll(callback): Get a list of all tags which have been applied.
//    tagsGetImagesTags(imagesIdsArray, callback): Return all tags associated with a set of images.
//    sendToTrash(oidArray, callback): Flag a set of images as being "in trash".
//    restoreFromTrash(oidArray, callback): Reset the "trash state" of a set of images such they are no
//      longer considered as being in trash.
//    viewTrash(options, callback): Return a set of images which are considered to be in trash.
//    deleteImages(oidArray, callback): Delete a set of images permanently.
//    emptyTrash(callback): Deletes all images which are curently considered as being "in trash"
//
//    importBatchFs(importDir, options, callback): Creates an import batch from images 
//      found on the filesystem.
//    importBatchShow(oid, options, callback): Retrieve an import batch.
//    importBatchUpdate(oid, options, callback): Update an import batch. Note, only the
//      'state' attribute may be modified.
//    importBatchFindRecent(N, options, callback): Retrieve N of the most recent import batches.
//
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
  },
  //
  // Mime types which will be allowed during import.
  //
  importMimeTypes : {
    image: ['jpeg', 'png']
  }
};

//
// Get a singleton instance of the storage module.
//
var mmStorage = require('MediaManagerStorage')();
var touchdb = mmStorage.get('touchdb');

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

priv.getBatchInProcess = function(oid) {
  var b = undefined;
  if (_.has(priv.batch_in_process, oid)) {
    b = priv.batch_in_process[oid];
  }
  return b;
};

priv.markBatchInit = function (anImportBatch) {
  priv.batch_in_process[anImportBatch.oid] = anImportBatch;
  anImportBatch.setStatus(anImportBatch.BATCH_INIT);
};

priv.markBatchAbortRequested = function(anImportBatch) {
  if (_.isObject(anImportBatch)) {
    if (_.has(priv.batch_in_process, anImportBatch.oid)) {
      priv.batch_in_process[anImportBatch.oid].setStatus(anImportBatch.BATCH_ABORT_REQUESTED);
    }
    anImportBatch.setStatus(anImportBatch.BATCH_ABORT_REQUESTED);
  }
}

priv.markBatchAborting = function(anImportBatch) {
  if (_.isObject(anImportBatch)) {
    if (_.has(priv.batch_in_process, anImportBatch.oid)) {
      priv.batch_in_process[anImportBatch.oid].setStatus(anImportBatch.BATCH_ABORTING);
    }
    anImportBatch.setStatus(anImportBatch.BATCH_ABORTING);
  }
}

priv.markBatchAborted = function(anImportBatch) {
  if (_.isObject(anImportBatch)) {
    if (anImportBatch.status === anImportBatch.BATCH_ABORTING) {
      if (_.has(priv.batch_in_process, anImportBatch.oid)) {
        delete priv.batch_in_process[anImportBatch.oid];
      }
      //
      // This will set the batch as aborted!
      //
      anImportBatch.setCompletedAt(new Date());
    }
    else {
      log.warn('Batch status is NOT BATCH-ABORTING, can\'t mark as ABORTED, batch - %j!', anImportBatch);
    }
  }
  else {
    log.warn("Illegal Argument in markBatchComplete: '%s'", anImportBatch);
  }
}

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
                   // log.debug('bulkDocFetch: Fetched ' + body.rows.length + ' documents, first doc - ' + util.inspect(body.rows[0].doc));
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
  // log.debug('runView: design doc. - ' + designDoc + ', view name - ' + viewName + ', options ' + util.inspect(options));
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

/*
 * create: Create a set of images given an initial set of attributes.
 *  Note, a singular attribute object can be provided to create
 *  a single instance of an image.
 *
 *  Args:
 *    imgAttrs: An instance or array of objects containing initial image 
 *      attributes.
 *    callback: Invoked upon completion with following signature - 
 *
 *      callback(err, images, errors),
 *
 *        where:
 *          err: error if nothing useful happened.
 *          images: is an image doc. or array of image documents successfully created.
 *          errors: When err is undefined, this is an array of errors associated with individual images.
 *            {
 *              attrs: attrs for the image.
 *              err: error for the image.
 *            }
 */
function create(imgAttrs, options, callback) {
  callback = callback || ((options && _.isFunction(options))?options:undefined);
  options = (options && !_.isFunction(options))?options:{};

  options.parse = (_.has(options, 'parse'))?options.parse:true;

  var imageToCreate = function(attrs) {
    // log.debug('create.imageToCreate: attrs - ' + util.inspect(attrs));
    attrs.oid = priv.genOid();
    if (config.app && _.has(config.app, 'id')) {
      attrs.app_id = config.app.id;
    }

    return mmStorage.docFactory('plm.Image', attrs);
  }

  var toCreate = undefined;

  if (_.isArray(imgAttrs)) {
    log.debug('create: imgAttrs is an array...');
    toCreate = _.map(imgAttrs, imageToCreate);
  }
  else {
    toCreate = [ imageToCreate(imgAttrs) ];
  }

  if (_.isArray(toCreate) && toCreate.length) {
    var created = [];
    var errors = [];
    async.eachSeries(toCreate, 
                     function(toC, innerCallback) {
                       parseImage(toC, 
                                  {verbose: false},
                                  function(err) {
                                    log.debug('create: parsed image, path - ' + toC.path);
                                    if (err) {
                                      errors.push({
                                        err: err,
                                        attrs: toC
                                      });
                                    }
                                    else {
                                      created.push(toC);
                                    }
                                    innerCallback(null);
                                  });
                     },
                     function(err) {
                       if (err) {
                         callback(err, []), errors;
                       }
                       else if (created.length === 0) {
                         callback("No images were created!", [], errors);
                       }
                       else {
                         var toStore = _.map(created, function(toC) {
                           var toS = toCouch(toC);

                           toS._id = toC.oid;

                           return toS;
                         });

                         var db = priv.db();

                         db.bulk({"docs": toStore}, 
                                 {include_docs: true},
                                 function(err, body) {
                                   if (err) {
                                     callback && callback('Error occurred in bulk document creation, error - ' + err);
                                   }
                                   else {
                                     log.debug('create: bulk document create response body - ' + util.inspect(body));
                                     
                                     if (!_.isArray(body) || (created.length != body.length)) {
                                       callback && callback('Error in image document creation, images to created did not equal number created!');
                                     }
                                     else {
                                       var createdIds = _.pluck(body, 'id');
                                       bulkDocFetch(createdIds,
                                                    function(err,
                                                             created) {
                                                      var cImgs = _.map(created, function(cImg) {
                                                        var tmp = mmStorage.docFactory('plm.Image', cImg);
                                                        priv.setCouchRev(tmp, cImg);
                                                        return tmp;
                                                      });
                                                      callback(err, cImgs, errors);
                                                    });
                                     }
                                   }
                                 });
                       }});
  }
  else {
    callback('No valid images to create!');
  }
}

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
        log.debug('save: saved ' + aryResult.length + ' images.');
        if(options && options.retrieveSavedImage){
          log.debug(util.format("save: After save, retrieving image '%s' by oid '%s'", anImgPath, aryResult[0].oid));
          show(aryResult[0].oid, null,next);
        } else {
          log.debug('save: no need to retrieve saved image.');
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
      log.info(util.format("Saved image '%s': '%j'", theSavedImage.name, theSavedImage));

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
 * parseAndTransform: When provided with an image path, or initial image document, 
 *  an optional stream, and a set of transforms, instantiate a gm object, and pass 
 *  it to parseImage for each variant. Invokes the callback with an array of 
 *  ImageData/Stream to persist.
 *
 *  Args:
 *    imgOrPath:
 *    options:
 *      parse: Whether to parse or not. Default: true.
 *    callback:
 */
function parseAndTransform(imgOrPath, options, callback)
{
  callback = callback || ((options && _.isFunction(options))?options:undefined);
  options = (options && !_.isFunction(options))?options:{};
  options.parse = (_.has(options, 'parse'))?options.parse:true;

  var image;
  var imgPath;

  if (_.isObject(imgOrPath)) {
    image = imgOrPath;
    imgPath = image.path;
  }
  else {
    image = undefined;
    imgPath = imgOrPath;
  }

  if (!_.isFunction(callback)) throw "parseAndTransform is not very useful if you don't provide a valid callback";

  var saveOriginal = options && _.has(options, 'saveOriginal') ?
    options.saveOriginal : true;

  var variants     = options && _.has(options, 'desiredVariants') && _.isArray(options.desiredVariants) ?
    options.desiredVariants : [];

  var batchId = options && _.isString(options.batch_id) ? options.batch_id : '' ;

  var aryPersist = [];

  step(
    function() {
      if ((image !== undefined) && !options.parse) {
        log.debug(' parseAndTransform: no image parsing required for image w/ path - ' + imgPath);
        this(null, image, imgPath);
      }
      else {
        parseImage(imgOrPath,
                   {verbose: false},
                   this);
      }
    },

    function(err, theImgMeta, theImgPath) {
      if (err) { throw err;}

      theImgMeta.batch_id = theImgMeta.batch_id || batchId;

      if (saveOriginal) {
        aryPersist.push({ data: theImgMeta, stream: theImgPath });
      } else {
        aryPersist.push({ data: theImgMeta });
      }

      if (!_.isObject(variants[0])) {
        // we are done
        log.debug("No variants to process for '%s'", imgPath);
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
          log.info("Done generating %s variants for image '%s'", variants.length, imgPath);
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
        parseImage(aTmpFileName, 
                   {verbose: false},
                   next);
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
 * Args:
 *  imgOrPath:
 *  options:
 *    verbose: Perform verbose processing, default is false.
 *  callback:
 *
 * TODO: move this method to an ImageProcessor instance
 * TODO: need a version of this method that returns the gm object so that variants can be generated by
 * re-using the original object
 */
function parseImage(imgOrPath, options, callback)
{
  callback = callback || ((options && _.isFunction(options))?options:function(){});
  options = (options && !_.isFunction(options))?options:{verbose:false};

  if (!_.isFunction(callback)) throw "parseImage is not very useful if you don't provide a valid callback";

  var imgPath = undefined;
  var imageMeta = undefined;

  if (_.isString(imgOrPath)) {
    imgPath = imgOrPath;

    var attrs = {path:imgPath, oid: priv.genOid()};
    if (config.app && _.has(config.app, 'id')) {
      attrs.app_id = config.app.id;
    }
    imageMeta = mmStorage.docFactory('plm.Image', attrs);

    // log.debug('parseImage: Created new image meta data - ' + util.inspect(imageMeta));

  }
  else {
    imageMeta = imgOrPath;
    imgPath = imageMeta.path;
    // log.debug('parseImage: Parsing image - ' + util.inspect(imageMeta));
  }

  var gmImg   = gm(fs.createReadStream(imgPath));

  step(
    function () {
      // the 'bufferStream: true' parm is critical, as it buffers the file in memory
      // and makes it possible to stream the bits repeatedly
      if (options.verbose) {
        log.debug("Verbose parsing of image file '%s'", imgPath);
        gmImg.identify({bufferStream: true},this);
      }
      else {
        var doIdentify = function(callback) {
          //
          // Do the equivalent of:
          //
          //  gm identify -format '{"format": %m, "size": {"width": %w, "height": %h}, "Geometry": "%wx%h", Filesize: %b}' '/Users/marekjulian/Projects/PLM/MediaManager/TestData/Test5/L1000932.jpg'
          //  {"format": JPEG, "size": {"width": 3468, "height": 5212}, "Geometry": "3468x5212", Filesize: 9.9M}
          //
          log.debug("Formatted parsing of image file '%s'", imgPath);
          var format = '{"format": "%m", "size": {"width": %w, "height": %h}, "Geometry": "%wx%h", "Filesize": "%b"}';
          gmImg.identify({format: format, bufferStream: true},
                         function(err, data) {
                           if (err) {
                             log.debug('Formatted parsing error of image file, error - ' + err);
                             callback(err);
                           }
                           else {
                             try {
                               var jData = JSON.parse(data);
                               callback(err, jData);
                             }
                             catch(e) {
                               log.error('Formatted parsing JSON data conversion - ' + e + ', data - ' + data);
                               callback('Invalid JSON returned while parsing image file - ' + imgPath);
                             }          
                           }
                         });
        };
        doIdentify(this);
      }
    },

    function (err, data) {
      if (err) { if (_.isFunction(callback)) callback(err); return; }
      log.debug("creating metadata for file '%s'", imgPath);
      imageMeta.readFromGraphicsMagick(data);
      gmImg.stream(this);
    },

    function (err, anImgStream, anErrStream) {
      log.debug("calculating checksum for file '%s'", imgPath);
      if (config.processingOptions.genCheckSums) {
        cs.gen(anImgStream, this);
      }
      else {
        this();
      }
    },

    function (aString) {
      log.debug("checksum for file '%s' is: %s", imgPath, aString);
      imageMeta.checksum = aString;
      // log.trace("checksumed image: " + JSON.stringify(imageMeta,null,"  "));
      // gmImg.stream(this);
      callback(null, imageMeta, imgPath);
    }
  );
}

/**
 * Takes an array of persist commands (see 'persist' function description), and invokes them in
 * series, returns an array with the image saved, or the corresponding error
 * TODO: error handling needs to be tested further
 */
function persistMultiple(aryPersist, aryResult, options, callback)
{
  callback = callback || ((options && _.isFunction(options)) ? options : function(){ log.debug('persistMultiple: default callback...');});
  options = (options && !_.isFunction(options))?options:{skipDoc:false};
  log.debug('persistMultiple: persisting ' + aryPersist.length + ' images.');
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

  async.eachSeries(aryPersist, iterator, function(err) {
    if (err) {
      var errMsg = util.format("Error happened while saving image and its variants: %s", err);
      log.error('persistMultiple: ' + errMsg);
      callback(errMsg);
    } else {
      log.debug('persistMultiple: done persisting...');
      callback(null, aryResult);
    }
  });


  function iterator(element, next) {
    persist(element, options, function(err, image) {
      log.debug('persistMultiple: pushing result, err - ' + err);
      aryResult.push( err ? err : image );
      next();
    });
  }
}


/*
 * persist: Takes a persistCommand and saves the image record to persistent storage
 *
 *  Args:
 *
 *    persistCommand:
 *
 *      { data: anImage, stream: aPath }
 *
 *    options:
 *      skipDoc: Skip saving the document, and only saves any relevant attachments.
 *    callback(err, imgData):
 *
 * The stream parameter is optional. If passed, the method expects to read the bits of the image at
 * that location, and will save them as an attachment to the image document. Otherwise, only the
 * image's metadata is persisted
 *
 * This should be moved to a DAO class that is couchdb-specific
 */
function persist(persistCommand, options, callback)
{
  callback = callback || ((options && _.isFunction(options))?options:function(){});
  options = (options && !_.isFunction(options))?options:{skipDoc:false};

  log.debug('persist: arguments - ' + util.inspect(arguments) + ', options - ' + JSON.stringify(options) + ', callback - ' + util.inspect(callback));

  var
    db = nano(
      {
        url: 'http://' + config.db.host + ':' + config.db.port + '/' + config.db.name,
        log: nanoLogFunc
      }
    )
    ,imgData   = persistCommand.data
    ;

  log.debug('persist: have DB, and image data - ' + JSON.stringify(imgData));
  async.waterfall(
    [
      function (next) {
        if (!options.skipDoc) {
          log.debug(util.format("persist: saving %j to db...", imgData));
          db.insert(toCouch(imgData), 
                    imgData.oid, 
                    function(err, body, headers) {
                      log.debug(util.format("persist: result from insert: %j, err - %s.", body, err ));
                      priv.setCouchRev(imgData, body);
                      log.debug('persist: set rev - ' + imgData._storage.rev);
                      next(err);
                    });
        }
        else {
          log.trace('persist: skipping doc.');
          next(null);
        }
      },

      function (next) {

        if (_.isString(persistCommand.stream)) {
          var attachName = imgData.name;

          log.debug(util.format("streaming image bits for file '%s' from path '%s' to storage device", attachName, persistCommand.stream));

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
                {rev: imgData._storage.rev}, next)
            );
          }
          catch(e) { log.error(util.format("error while streaming: %j", e));}
        } else {
          log.debug('persist: No image bits to save...');
          callback(null, imgData);
          return;
        }
      },

      function(body, headers, next) {
        log.debug('persist: saved image as attachment...');

        // clean-up work directory if this was a temp file generated in the workDir
        if ( persistCommand.isTemp && _.isString(persistCommand.stream) ) {
          fs.unlink(persistCommand.stream, function(err) {
            if (err) { log.warn("error when deleting '%s' from workDir: %j", persistCommand.stream, err); }
          });
        }
        // imgData._storage.rev = body.rev;
        priv.setCouchRev(imgData, body);
        next(null);
      }
    ],
    function(err) {
      log.debug('persist: completed, err - ' + err);
      callback(err, imgData);
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
 *  options:
 *    - recursionDepth: 0,    // by default performs full recursion, '1' would process only the files inside the target_dir
 *    - ignoreDotFiles: true, // by default ignore .dotfiles
 *    - all options that can be passed to ImageService.save() which will be applied to all images in
 *      the import batch
 *
 *  callback(err, importBatch): is invoked with the initialized importBatch, and processing of the batch will be
 *    triggered asynchronously. importBatchShow(oid) can be called to monitor the progress of the
 *    importPatch's processing.
 *
 *    err: Object containing -
 *      code
 *      message
 *
 *    See exports.errors below for a list of errors which are return.
 */
function importBatchFs(target_dir, callback, options)
{
  var lp = 'importBatchFs: ';

  options = options || {};
  var smallestFirst = true;
  var db = priv.db();
  var importBatch = undefined;

  //
  // toProcessBatchSize: We process N images, then persist the N in bulk. Default to 10.
  //
  var toProcessBatchSize = (config.processingOptions && config.processingOptions.toProcessBatchSize) ? 
    config.processingOptions.toProcessBatchSize : 10;

  //
  // imageStatus:
  //
  //  image.oid ->
  //    {
  //      status: 0 - success.
  //      err: any error.
  //      image: image.
  //    }
  //
  var imageStatus = {};

  var finalSaveAttempted = false;

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
          
          var err = _.clone(errors.NO_FILES_FOUND);
          err.message = util.format(err.message, target_dir);

          if (_.isFunction(callback)) {
            callback(err);
          }

          next(err.message);
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

      //
      // Mark the batch as initialized, and create an initial set of images.
      //
      function (body, headers, next) {
        priv.setCouchRev(importBatch, body);
        log.debug(lp + "Saved importBatch record to db before initial image generation:  id '%s' -  rev '%s'", importBatch.oid, importBatch._storage.rev);

        priv.markBatchInit(importBatch);

        // return the initialized importBatch...
        if (_.isFunction(callback)) {
          callback(null, importBatch);
        }

        importBatch.setStartedAt(new Date());

        next(undefined);
      },

      //
      // Pass 1 thru batch images, processing N images, where N is toProcessBatchSize:
      //
      //  - create N initial image docs which get persisted in bulk.
      //  - processBatchImages on those N new images generating smallest desired variant ONLY.
      //    - emit import.images.variant.created for the N which were resized.
      //    
      //
      function(next) {

        if (importBatch.status === importBatch.BATCH_ABORT_REQUESTED) {
          log.debug(lp + 'Batch abort requested, skipping initial batch processing...');
          priv.markBatchAborting(importBatch);
          next(undefined, []);
        }

        var imageAttrs = [];

        _.each(importBatch.images_to_import, function(imageToImport) {
          imageAttrs.push({ path: imageToImport.path,
                            format: imageToImport.format,
                            batch_id: importBatch.oid
                          });
        });

        if (imageAttrs.length > 0) {

          var taskHadError = false;

          //
          // Pass 1 processBatchImages options, create them once for all batches queued.
          //
          var processOptions = _.clone(options);
          processOptions.parse = false;
          if (smallestFirst) {
            processOptions.desiredVariants = options && _.has(options, 'desiredVariants') && _.isArray(options.desiredVariants) ?
              [ _.reduce(options.desiredVariants, function(memo, v) { return !memo ? v : (((v.width * v.height) < (memo.width * memo.height)) ? v : memo); }) ] : [];
          }
          else {
            processOptions.desiredVariants = options && _.has(options, 'desiredVariants') && _.isArray(options.desiredVariants) ?
              [ _.reduce(options.desiredVariants, function(memo, v) { return !memo ? v : (((v.width * v.height) > (memo.width * memo.height)) ? v : memo); }) ] : [];
          }

          //
          // Q batches to do with concurrency 1.
          //
          //  task:
          //    imageAttrs: array of image attributes in the batch.
          //
          var q = async.queue(
            function(task, taskCallback) {
              if (taskHadError) {
                taskCallback('Aborting pass 1 image processing due to previous processing error.');
              }
              else if (importBatch.status === importBatch.BATCH_ABORT_REQUESTED) {
                log.debug(lp + 'Batch abort requested, aborting pass 1 of batch processing...');
                priv.markBatchAborting(importBatch);
                taskCallback('Aborting pass 1 as batch abort request detected!');
              }
              else if (importBatch.status === importBatch.BATCH_ABORTING) {
                log.debug(lp + 'Batch processing aborting, skipping initial batch processing...');
                taskCallback('Aborting pass 1 as batch status is ABORTING!');
              }
              else {
                async.waterfall(
                  [
                    //
                    // Create N images for the batch.
                    //
                    function(pass1Next) {
                      create(task.imageAttrs, 
                             {parse: true},
                             function(err, created, errors) {
                               if (errors) {
                                 _.each(errors, function(error) {
                                   imageStatus[error.attrs.id] = {
                                     status: -1,
                                     err: error.err,
                                     image: error.attrs
                                   };
                                 });
                               }
                               if (created && created.length) {
                                 pass1Next(null, created);
                               }
                               else if (err) {
                                 pass1Next(err);
                               }
                               else {
                                 pass1Next("No images were created!");
                               }
                             });
                    },
                    //
                    // Update the set of created images associated with the batch, and
                    // start processing them.
                    //
                    function(createdImages, pass1Next) {
                      log.info("importBatchFs: Successfully created " + createdImages.length + ' batch images found in dir - ' + importBatch.path + ', for batch images ' + task.begin + ' to ' + (task.end-1) + '...');

                      _.each(createdImages,
                             function(img) {
                               imageStatus[img.oid] = {
                                 status: 0,
                                 err: undefined,
                                 image: img
                               };
                             });
                      
                      importBatch.addCreated(createdImages);
                      
                      log.info('Starting pass 1 import batch processing for path - ' + importBatch.path + ', for batch images ' + task.begin + ' to ' + (task.end-1) + '...');

                      //
                      // And continue processing the created images BUT only create the smallest (largest) variant (see processOptions above).
                      //
                      processOptions.images = createdImages;
                      processBatchImages(importBatch, processOptions, function(err, processed) {
                        log.info('Pass 1 import batch processing for path - ' + importBatch.path + ', for batch images ' + task.begin + ' to ' + (task.end-1) + 'completed, processed - ' + processed.length + ' ...');
                        _.each(processed, function(imgStatus) {
                          if (imgStatus.status !== 0) {
                            imageStatus[imgStatus.image.oid].status = imgStatus.status;
                            imageStatus[imgStatus.image.oid].err = imgStatus.err;
                          }
                          imageStatus[imgStatus.image.oid].image = imgStatus.image;
                        });
                        log.info('Pass 1 import batch processing for path - ' + importBatch.path + ', for batch images ' + task.begin + ' to ' + (task.end-1) + 'completed, updated image status...');
                        pass1Next(err);
                      });
                    }
                  ],
                  function(err) {
                    if (err) {
                      taskHadError = true;
                    }
                    taskCallback(err);
                  });
              }
            }, 1);

          q.drain = function() { 
            log.info('Pass 1 processing of import batch completed...');
            if (taskHadError) {
              next('Errors during pass 1 import batch processing...');
            }
            else {
              next(undefined, importBatch.images);
            }
          };

          var end;

          for (var batchNum = 1, begin = 0; begin < imageAttrs.length; ++batchNum, begin = begin + toProcessBatchSize) {
            end = ((begin + toProcessBatchSize) < imageAttrs.length) ? begin + toProcessBatchSize : imageAttrs.length;

            q.push({ imageAttrs: imageAttrs.slice(begin, end),
                     batchNum: batchNum,
                     begin: begin,
                     end: end
                   },
                   function(err) {
                     if (err) {
                       taskHadError = true;
                       log.error('Error during pass 1 processing of import batch for batch images ' + begin + ' to ' + (end - 1) + '!');
                     }
                     else {
                       log.info('Pass 1 processing of import batch successful for batch images ' + begin + ' to ' + (end - 1) + '...');
                     }
                   });
          }
        }
        else {
          next(undefined, []);
        }
      },

      //
      // Save the batch after initial processing.
      //
      function(images, next) {
        db.insert(importBatch, importBatch.oid, function(err, body, headers) {
          if (err) {
            log.error(lp + 'Error saving batch after pass 1, err - ' + err);
            next('Error saving batch after pass 1 processing.');
          }
          else {
            priv.setCouchRev(importBatch, body);
            next(undefined, images);
          }
        });
      },

      //
      // Pass 2 thru batch images:
      //  - invoke processBatchImages on any remaining image variants.
      //
      function(images, next) {
        if (importBatch.status === importBatch.BATCH_ABORT_REQUESTED) {
          log.debug(lp + 'Batch processing aborting, skipping pass 2 of batch processing...');
          priv.markBatchAborting(importBatch);
          next(undefined);
        }
        else if (importBatch.status === importBatch.BATCH_ABORTING) {
          log.debug(lp + 'Batch abort requested, aborting pass 2 of batch processing...');
          next(undefined);
        }
        if (images && images.length) {
          //
          // Create the remaining image variants, setting up the processing options.
          //
          var processOptions = _.clone(options);
          processOptions.parse = false;

          var optV = options && _.has(options, 'desiredVariants') && _.isArray(options.desiredVariants) ? options.desiredVariants : [];
          var createdVariant = undefined;
          if (smallestFirst) {
            createdVariant = optV.length ? _.reduce(optV, function(memo, v) { return !memo ? v : (((v.width * v.height) < (memo.width * memo.height)) ? v : memo); }) : { name: undefined };
          }
          else {
            createdVariant = optV.length ? _.reduce(optV, function(memo, v) { return !memo ? v : (((v.width * v.height) > (memo.width * memo.height)) ? v : memo); }) : { name: undefined };
          }

          processOptions.desiredVariants = _.filter(optV, function(v) { return v.name != createdVariant.name; });

          //
          // Once again, queue the batches within the batch like in pass 1 to generate remaining variants.
          // Also, we bail if there were any errors.
          //
          var taskHadError = false;

          var q = async.queue(
            function(task, taskCallback) {
              if (taskHadError) {
                taskCallback('Aborting pass 2 image processing due to previous processing errors for batch images ' + task.begin + ' to ' + (task.end-1) + '!');
              }
              else if (importBatch.status === importBatch.BATCH_ABORT_REQUESTED) {
                log.debug(lp + 'Batch abort requested, aborting pass 2 of batch processing...');
                priv.markBatchAborting(importBatch);
                taskCallback('Aborting pass 2 as batch abort request detected!');
              }
              else if (importBatch.status === importBatch.BATCH_ABORTING) {
                log.debug(lp + 'Batch processing aborting, aborting pass 2 of batch procesing...');
                taskCallback('Aborting pass 2 as batch status is ABORTING!');
              }
              else {
                log.info('Starting pass 2 importBatch processing for path - ' + importBatch.path + ', for batch images ' + task.begin + ' to ' + (task.end-1) + '...');

                processOptions.images = importBatch.images.slice(task.begin, task.end);
                processBatchImages(importBatch, 
                                   processOptions, 
                                   function(err, processed) {
                                     if (err) {
                                       taskHadError = true;
                                     }
                                     _.each(processed, function(imgStatus) {
                                       if (imgStatus.status !== 0) {
                                         imageStatus[imgStatus.image.oid].status = imgStatus.status;
                                         imageStatus[imgStatus.image.oid].err = imgStatus.err;
                                       }
                                       imageStatus[imgStatus.image.oid].image = imgStatus.image;
                                     });

                                     log.debug('Retrieving batch images, name - ' + importBatch.oid + ', rev - ' + importBatch._storage.rev);

                                     async.eachSeries(processed, 
                                                      function(imgStatus, innerCallback) {
                                                        var overallStatus = imageStatus[imgStatus.image.oid];

                                                        if (overallStatus.status === 0) {
                                                          show(imgStatus.image.oid, null, function(err, savedImage) {
                                                            if (err) {
                                                              overallStatus.status = -1;
                                                              overallStatus.err = err;
                                                            }
                                                            else {
                                                              importBatch.addSuccess(savedImage);
                                                            }
                                                            innerCallback(null);
                                                          });
                                                        }
                                                        else {
                                                          log.error("Error while processing image at '%s': %s", imgStatus.image.path, err);
                                                          innerCallback(null);
                                                        }
                                                      },
                                                      function(err) {
                                                        log.debug('processBatchImages.processBatchImage: batch - %j, successes - %d', importBatch, importBatch.getNumSuccess());
                                                        taskCallback(null, processed);
                                                      });

                                   });
              }
            }, 1);

          q.drain = function() {
            log.info('Pass 2 processing of import batch completed...');
            if (taskHadError) {
              next('Errors during pass 2 import batch processing...');
            }
            else {
              next(undefined);
            }
          };

          var end;

          for (var batchNum = 1, begin = 0; begin < importBatch.images.length; ++batchNum, begin = begin + toProcessBatchSize) {
            end = ((begin + toProcessBatchSize) < importBatch.images.length) ? begin + toProcessBatchSize : importBatch.images.length;

            q.push({ batchNum: batchNum,
                     begin: begin,
                     end: end },
                   function(err, processedImages) {
                     if (err) {
                       log.error('Error during pass 2 processing of import batch for batch images ' + begin + ' to ' + (end - 1) + '!');
                     }
                     else {
                       log.info('Pass 2 processing of import batch successful for batch images ' + begin + ' to ' + (end - 1) + '...');
                     }
                   });
          }
        }
        else {
          next();
        }
      }
    ],
    function(err) {
      _.each(imageStatus, function(imgStatus) {
        if (imgStatus.status !== 0) {
          importBatch.addErr(imgStatus.image.path, imgStatus.err);
        }
      });
      if (importBatch) {
        priv.markBatchComplete(importBatch);
        if (err) {
          var errMsg = util.format("Error while processing importBatchFs '%s': %s", importBatch.oid,err);
          log.error(lp + errMsg);
        }
        //
        // Save the batch, regardless of errors...
        //
        log.debug(lp + 'Saving batch, name - ' + importBatch.oid + ', rev - ' + importBatch._storage.rev);

        db.insert(importBatch, importBatch.oid, function(err, body, headers) {
          if (err) {
            log.error(lp + "Error while saving import batch '%s': %s", importBatch.path, err);
          } else {
            if (log.isTraceEnabled()) { log.trace(lp + "result of batch save: %j", body); }
            priv.setCouchRev(importBatch, body); // just in case
            log.info(lp + "Successfully saved import batch '%s': %j", importBatch.path, importBatch);
          }
        });
      }
    }
  );
}
exports.importBatchFs = importBatchFs;

/*
 * processBatchImages: A batch already has iamges associated with it.
 *  Process the images, and re-persist the image documents.
 *
 *  Args:
 *    batch:
 *    options:
 *      images: Only process these images, and NOT all of them.
 *      parse: Whether to parse or NOT.
 *    callback(err, processed):
 *      processed: array of -
 *        {
 *          status: 0 - success, otherwise, an error.
 *          err: any error.
 *          image: image.
 *          toPresist: list of items to persist.
 *         }
 */
function processBatchImages(batch, options, callback) {
  var opts = options || {};

  var imgsToP = _.has(options, 'images') ? options.images : batch.images;

  var numErrors = 0;
  var processed = [];

  log.debug(' Processing images in import batch, ' + imgsToP.length + ' images to process, with options - ' + util.inspect(options));

  var numJobs = 1;

  if (config.processingOptions && config.processingOptions.numJobs) {
    numJobs = config.processingOptions.numJobs;
  }

  log.debug('processBatchImages: Using jobs - ' + numJobs);

  var q = async.queue(
    function(task, taskCallback) {
      parseAndTransform(task.image, 
                        options, 
                        function(err, toPersist) {
                          var pItem = undefined;
                          if (err) {
                            numErrors = numErrors + 1;
                            pItem = {
                              status: -1,
                              err: err,
                              image: task.image,
                              toPersist: toPersist
                            };
                          }
                          else {
                            pItem = {
                              status: 0,
                              err: undefined,
                              image: task.image,
                              toPersist: toPersist
                            };
                            processed.push(pItem);
                          }
                          taskCallback(err, pItem);
                        });
    }, numJobs);

  q.drain = function() {
    //
    // Done, bulk persist everything.
    //
    var db = priv.db();

    async.waterfall(
      [
        //
        // First we bulk persist the documents.
        //
        function(drainNext) {

          var toStore = [];
          var persistMap = {};

          _.map(processed, function(pImg) {
            _.each(pImg.toPersist, function(toP) {
              var toS = toCouch(toP.data);

              toS._id = toP.data.oid;
            
              toStore.push(toS);

              persistMap[toS._id] = toP;
            });
          });

          db.bulk({"docs": toStore}, 
                  {include_docs: true},
                  function(err, body) {
                    if (err) {
                      _.each(processed, function(pImg) {
                        pImg.status = -1;
                        pImg.err = err;
                      });
                    }
                    else {
                      _.each(body, function(bulkRow) {
                        priv.setCouchRev(persistMap[bulkRow.id].data, bulkRow);
                      });
                    }
                    drainNext(err);
                  });
        },
        //
        // Individual Docs where saved, and rev's updated. Now we need to persist
        // any attachments.
        //
        function(drainNext) {
          if (_.has(options, 'desiredVariants')) {
            async.waterfall(
              [
                function(drainNextInner) {
                  async.eachSeries(processed, 
                               function(pImg, pmCallback) {
                                 var pResult = [];
                                 persistMultiple(pImg.toPersist, 
                                                pResult,
                                                {skipDoc:true},
                                                pmCallback);
                               },
                               function(err) {
                                 drainNextInner(err);
                               });
                },
                function(drainNextInner) {
                  var docsToReturn = [];

                  async.eachSeries(processed,
                                   function(pItem, showCallback) {
                                     show(pItem.image.oid, null, function(err, docToReturn) {
                                       if (err) {
                                         pItem.status = -1;
                                         pItem.err = err;
                                       }
                                       else {
                                         docsToReturn.push(docToReturn);
                                         pItem.image = docToReturn;
                                       }
                                       showCallback(err);
                                     });
                                   },
                                   function(err) {
                                     batch.addVariantCreated(docsToReturn);
                                     drainNextInner(err);
                                   });
                }
              ],
              function(err) {
                drainNext(err);
              });
          }
          else {
            drainNext(null);
          }
        }
      ],
      function(err) {
        callback(err, processed);
      }
    );
  };

  _.each(imgsToP, function(imgToP) {
    q.push({image: imgToP},
           function(err, pItem) {
           });
  });
}

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

/*
 *
 * importBatchUpdate(attr, options, callback): Update an import batch. Note, only the
 *  'status' attribute may be modified. First a 'show' of the batch is performed, and
 *  the current version of the import batch is fetched. Then, the attributes supplied
 *  via the 'attr' paramater are compared with the latest. If any differ other than
 *  'status' attribute, the callback is invoked with a value of errors.CONFLICT. If only
 *  the 'status' attribute has changed, and is being updated from a value of 'started'
 *  to 'abort-requested', the import batched is updated and persisted. If an attempt
 *  to update the 'status' attribute in some other manner is being made, then 
 *  the callback will be invoked with errors.ATTRIBUTE_VALIDATION_FAILURE.
 *
 *  Args:
 *    attr: Attributes of the batch. Must at least contain 'oid'
 *    options: none
 *    callback(err, batch): Invoked with err or on success with the updated batch.
 */
function importBatchUpdate(attr, options, callback) {
  var lp = 'importBatchUpdate: ';

  log.debug(lp + 'Updating with attributes - ' + JSON.stringify(attr));
  if (!_.has(attr, 'oid') || !_.has(attr, 'status')) {
    callback(errors.ATTRIBUTE_VALIDATION_FAILURE);
  }
  else {
    //
    // 1. Do a show to get the batch.
    // 2. Check for conflict in any attr values with what was fetched in 1.
    //   - Ensure only writable attributes differ.
    //   - If status is being updated, ensure it is going from STARTED -> ABORT-REQUESTED
    // 3. Update writable attributes.
    // 4. Persist back batch.
    //
    var writable = [ 'status' ];


    async.waterfall(
      [
        //
        // 1. Get the current state of the batch.
        //
        function(next) {
          var currBatch = priv.getBatchInProcess(attr.oid);

          if (currBatch) {
            log.debug(lp + 'Retrieve in process batch - ' + util.inspect(currBatch));
            next(null, currBatch);
          }
          else {
            importBatchShow(attr.oid,
                            { includeImages: false },
                            function(err, batch) {
                              if (err) {
                                next(errors.UNKNOWN_ERROR);
                              }
                              else {
                                currBatch = mmStorage.docFactory('plm.ImportBatch', batch);
                                next(null, currBatch);
                              }
                            });
          }
        },
        //
        // 2. Data validation.
        //
        function(currBatch, next) {
          var batchKeys = _.keys(currBatch);
          var notWritable = _.difference(batchKeys, writable);

          var haveError = false;

          _.each(notWritable, function(nw) {
            if (_.has(attr, nw) && (attr[nw] !== currBatch[nw])) {
              log.debug(lp + 'validation error, attr[' + nw + '] !== batch[' + nw + '], setting - ' + attr[nw] + ', current - ' + currBatch[nw]);
              haveError = true;
            }
          });
          if (!haveError && (currBatch.status !== attr.status) && (currBatch.status === 'STARTED') && (attr.status !== 'ABORT-REQUESTED')) {
            log.debug(lp + 'validation error, batch status - ' + currBatch.status + ', setting status to - ' + attr.status);
            haveError = true;
          }
          if (!haveError && (currBatch.status !== attr.status) && (currBatch.status !== 'STARTED') && (attr.status === 'ABORT-REQUESTED')) {
            log.debug(lp + 'validation error, batch status - ' + currBatch.status + ', setting status to - ' + attr.status);
            haveError = true;
          }
          if (haveError) {
            next(errors.ATTRIBUTE_VALIDATION_FAILURE);
          }
          else {
            log.debug(lp + 'Update request satisfied validation...');
            next(null, currBatch);
          }
        },
        //
        // 3. update writable attributes.
        //
        function(currBatch, next) {
          log.debug(lp + 'Ready to update batch attributes, batch - ' + util.inspect(currBatch));
          if (_.has(attr, 'status') && (currBatch.status !== attr.status) && (attr.status === currBatch.BATCH_ABORT_REQUESTED)) {
            log.debug(lp + 'Changing batch status to abort-requested...');
            priv.markBatchAbortRequested(currBatch);
          }
          _.each(writable, function(wAttr) {
            if (_.has(attr, wAttr)) {
              currBatch[wAttr] = attr[wAttr];
            }
          });

          next(null, currBatch);
        },
        //
        // 4. persist.
        //
        function(currBatch, next) {
          var db = priv.db();
          db.insert(currBatch, currBatch.oid, function(err, body, headers) {
            if (err) {
              log.error(lp + "Error while saving importBatch, oid - %s, err - %s", currBatch.oid, err);
              next(errors.UNKNOWN_ERROR, currBatch);
            } else {
              if (log.isTraceEnabled()) { log.trace(lp + "result of importBatch save: %j", body); }
              priv.setCouchRev(currBatch, body); // just in case
              log.info(lp + "Successfully saved importBatch, oid - %s, batch - %j, body - %j", currBatch.oid, currBatch, body);
              next(null, currBatch);
            }
          });          
        }
      ],
      function(err, result) {
        if (err) {
          log.error(lp + 'Error updating batch, error - ' + util.inspect(err));
        }
        else {
          log.debug(lp + 'Successfully updated batch - ' + JSON.stringify(result));
        }
        callback(err, result);
      }
    );
  }
};
exports.importBatchUpdate = importBatchUpdate;

/**
 * Lists the 'N' most recent import batches
 *
 * NOTE: this is replaced with importBatchIndex. Leaving here for debugging in case the new implementation
 *  produces questionable results.
 *
 * N: Number of batches to return. If undefined, all batches are returned.
 * options:
 *   includeImages: false by default, if true returns all images with variants that are part of the batch
 *   filterNoImages: Do not return any import batches with no associated images. Default: true.
 *   filterAllInTrash: Do not return any import batches where all images are in trash. Default: true.
 *   filterNotStarted: Do not return any import batches where the corresponding import process has not yet begun. Default: true.
 */
function importBatchFindRecent(N, options, callback) {
  var db = priv.db();
  var aryBatchOut = []; //
  var opts = _.isObject(options) ? options : {};
  var view = VIEW_BATCH_BY_CTIME;

  var filterOnImages = _.has(options, 'filterNoImages') && _.has(options, 'filterAllInTrash') && _.has(options, 'filterNotStarted');

  var view_opts = {
    descending: true
    ,include_docs: true
  };

  //
  // If not doing any filtering, set the limit.
  //
  if ((N !== undefined) && !filterOnImages) {
    view_opts.limit = N;
  }

  log.debug("Finding %s most recent importBatches with options %j", N, options);
  log.trace("Finding %s most recent importBatches using view '%s' with view_opts %j", N, view, view_opts);

  db.view(IMG_DESIGN_DOC, 
          view, 
          view_opts,
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

              if (opts.includeImages || filterOnImages) {
                importBatchRetrieveImages(aryBatchOut, options, function(err, out) {
                  if (!err) {log.debug(success);}

                  if (options.filterNotStarted) {
                    var outTmp = out;

                    out = _.filter(outTmp, function(imp) {
                      return _.has(imp, 'started_at') && imp.started_at;
                    });
                  }
                  
                  if (options.filterAllInTrash) {
                    var outTmp = out;
                    out = [];
                    _.each(outTmp, function(imp) {
                      var images = _.filter(imp.images, function(image) {
                        var include = true;

                        if (image.in_trash) {
                          include = false;
                        }

                        return include;
                      });
                      if (images.length) {
                        imp.images = images;
                        out.push(imp);
                      }
                    });
                  }

                  if (options.filterNoImages) {
                    var outTmp = out;

                    out = _.filter(outTmp, function(imp) {
                      return imp.images.length > 0;
                    });
                  }

                  if ((N !== undefined) && (out.images.length > N)) {
                    out = out.slice(0, N);
                  }
                  
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

/*
 * importBatchIndex: Lists the 'N' most recent import batches
 *
 * NOTE: this is replaced with importBatchIndex. Leaving here for debugging in case the new implementation
 *  produces questionable results.
 *
 * N: Number of batches to return. If undefined, all batches are returned.
 * options:
 *   includeImages: false by default, if true returns all images with variants that are part of the batch
 *   filterNoImages: Do not return any import batches with no associated images. Default: true.
 *   filterAllInTrash: Do not return any import batches where all images are in trash. Default: true.
 *   filterNotStarted: Do not return any import batches where the corresponding import process has not yet begun. Default: true.
 */
function importBatchIndex(N, options, callback) {

  var lp = 'importBatchIndex: ';

  callback = callback || ((options && _.isFunction(options))?options:undefined);
  options = (options && !_.isFunction(options))?options:{};

  if (!_.has(options, 'includeImages')) {
    options.includeImages = false;
  }

  if (!_.has(options, 'filterNoImages')) {
    options.filterNoImages = true;
  }
  if (!_.has(options, 'filterAllInTrash')) {
    options.filterAllInTrash = true;
  }
  if (!_.has(options, 'filterNotStarted')) {
    options.filterNotStarted = true;
  }

  var filterOnImages = 
    options.filterNoImages || 
    options.filterAllInTrash || 
    options.filterNotStarted;

  var transform = function(batch, callback) {
    if (options.includeImages || filterOnImages) {
      importBatchShow(batch.oid,
                      { includeImages: true },
                      function(err, newBatch) {
                        callback(err, newBatch);
                      });
    }
    else {
      callback(null, batch);
    }
  };

  var filterFunc = function(doc) {

    if (options.filterNotStarted) {
      if (!_.has(doc, 'started_at') || !doc.started_at) {
        log.debug(lp + 'Filtering import bactch w/ id - ' + doc.oid + ', reason - not started.');
        return true;
      }
    }

    if (options.filterAllInTrash) {
      if (_.has(doc, 'images')) {
        var nonTrash = _.find(doc.images, function(image) {
          return !image.in_trash;
        });

        if (!nonTrash) {
          log.debug(lp + 'Filtering import bactch w/ id - ' + doc.oid + ', reason - all in trash.');
          return true;
        }
      }
    }

    if (options.filterNoImages) {

      var numImages = _.has(doc, 'images') ? doc.images.length : 0;

      log.debug(lp + 'No image filter - ' + numImages);
      
      if (numImages === 0) {
        log.debug(lp + 'Filtering import batch w / id - ' + doc.oid + ', reason - no images.');
        return true;
      }
    }

    return false;
  };

  var nToFetch = N ? N : undefined;
  var dIt = new touchdb.DocIterator(
    nToFetch, 
    IMG_DESIGN_DOC, 
    VIEW_BATCH_BY_CTIME,
    {
      transform: transform,
      filterSync: filterFunc
    }
  );

  var docs = [];

  dIt.next().then(
    function(page) {
      log.debug(lp + 'Got ' + page.length + ' batches...');
      callback(null, page);
    },
    function(err) {
      log.error(lp + 'error - ' + err);
      callback(err);
    });
};

exports.importBatchIndex = importBatchIndex;
exports.importBatchFindRecent = importBatchIndex;

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

      if (_.has(config.importMimeTypes, mimeData[0]) && (config.importMimeTypes[mimeData[0]].indexOf(mimeData[1]) > -1)) {
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

  runView(IMG_DESIGN_DOC,
          VIEW_TRASH,
          {
            toReturn: 'docs',
            fetchDocs: true,
            fetchDocsBatchSize: 100,
            callback: function(err, docs) {

              if (!err) {
                log.debug('viewTrash.runView.callback: received ' + docs.length + ' documents!');
                //remove duplicates
                var resultDocs = _.uniq(docs, function (doc) {
                  return doc.oid;
                });

                async.eachSeries(resultDocs,
                                 function (doc, next) {
                                   var anImage = mmStorage.docFactory('plm.Image', doc);
                                   if(anImage.isOriginal()) {
                                     log.debug('viewTrash.runView.callback: retrieving doc w/id - ' + doc.oid);
                                     show(doc.oid,
                                          null,
                                          function(err,image){
                                            if (err) { 
                                              log.error('viewTrash.runView.callback: error retrieving image w/id - ' + doc.oid);
                                              callback(err); 
                                            }
                                            else {
                                              log.debug('viewTrash.runView.callback: Pushing image to result set - %j', image);
                                              aryImgOut.push(image);
                                            }
                                            next(null);
                                          }
                                         );
                                   }
                                   else{
                                     //is a variant, do nothing since the variant is already nested in the variants attribute of an original
                                     next();
                                   }
                                 },
                                 function(err) {
                                   if (err) {
                                     callback(err);
                                   }
                                   else {
                                     //all images were processed time to callback
                                     callback(null, aryImgOut);
                                   }
                                 }
                                );
              } 
              else {
                callback(util.format("error in viewTrash with view options '%j' - err: %s - body: %j", view_opts, err, body));
              }
            }
          });
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

//
// Communicating errors:
//

var errorCodes = {
  UNKNOWN_ERROR: -1,
  NO_FILES_FOUND: 1,
  CONFLICT: 2,
  ATTRIBUTE_VALIDATION_FAILURE: 3,
  NOT_IMPLEMENTED: 4
};
exports.errorCodes = errorCodes;

var errors = {
  UNKNOWN_ERROR: {
    code: errorCodes.UNKNOWN_ERROR,
    message: "Unknown error occurred."
  },
  NO_FILES_FOUND: {
    code: errorCodes.NO_FILES_FOUND,
    message: "No files found in directory %s"
  },
  CONFLICT: {
    code: errorCodes.CONFLICT,
    message: "Entity conflict, a revision of the entity has been generated with attribute values which would conflict with new values being set."
  },
  ATTRIBUTE_VALIDATION_FAILURE: {
    code: errorCodes.ATTRIBUTE_VALIDATION_FAILURE,
    message: "Attributes being set have failed validation."
  },
  NOT_IMPLEMENTED: {
    code: errorCodes.NOT_IMPLEMENTED,
    message: "Feature not implemented."
  }
};

exports.errors = errors;
