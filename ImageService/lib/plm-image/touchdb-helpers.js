//
// touchdb-helpers: Various TouchDB (, or CouchDB) helpers.
//
//  bulkDocFetch: Fetch a bunch of documents given a list of doc IDs.
//  fetchDocs: Fetch documents, optionally in batches.
//  runView: Run a view, and optionally including documents when running
//    the view, or separately fetching the documents.
//  iterateOverView: iterate over a view fetching documents.
//  iterateOverViewKeys: Repeatedly invoke runView given a set of view keys.
//    Useful for views that return many documents for the same key to avoid
//    issues with result set sizes coming from TouchDB. For a small number of
//    keys, or a small number of results for a reasonable number of keys,
//    just use runView().
//
'use strict';
var util   = require('util');
var _      = require('underscore');
var nano   = require('nano');
var log4js = require('log4js');
var async  = require('async');

//
// Try to get a singleton instance of the storage module.
//
var mmStorage = require('MediaManagerStorage')();
var touchdb = mmStorage.get('touchdb');

var tmp = __filename.split('/');
var fname = tmp[tmp.length-1];
var moduleName = fname.replace('.js', '');
var logPrefix = moduleName + ': ';

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

//
// map used to store all private attributes / functions
//
var priv = {
  host: undefined,
  port: undefined,
  database: undefined
};

var dbServer = null;

// returns a db connection
priv.db = function db() {
  log.trace("priv.db: Connecting to data base, host - '%s' - port '%s' - db '%s'", this.host, this.port, this.database);
  dbServer = dbServer || nano(
    {
      url: 'http://' + this.host + ":" + this.port,
      log: nanoLogFunc
    }
  );
  return dbServer.use(this.database);
};

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
function bulkDocFetch(docIds, callback) {
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
}

//
// fetchDocs: Fetch documents, optionally in batches.
//
//  Args:
//    docIds: Document IDs.
//    options:
//      batchSize: Batchsize to use. By default ALL documents will be fetched.
//      convertToImage: Convert to image objects. Default: false.
//      callback: callback(err, docs)
//
function fetchDocs(docIds, options) {
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
      if (!err && options.convertToImage) {
        log.debug('fetchDocs: Converting docs to images...');
        docs = convert_couch_body_to_array_of_images(null, docs);
      }
      callback && callback(err, docs);
    }
  );
}

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
//        'value': returns the value
//
//        default: 'ids', unless viewOptions.reduce === true. When reducing, 'value' will be returned.
//
//      viewOptions: Options to pass to the view, ie: startkey, etc.
//      fetchDocs: When toReturn is 'docs', fetch the docs separately. include_docs = true is NOT passed to the view.
//      fetchDocsBatchSize: When fetchDocs is specified, optionally specify a batchsize.
//      callback(err, results): Callback.
//
function runView(designDoc, viewName, options) {
  // log.debug('runView: design doc. - ' + designDoc + ', view name - ' + viewName + ', options ' + util.inspect(options));
  options = options || {};
  var viewOptions = options.viewOptions || {};

  if (!options.toReturn) {
    if (viewOptions.reduce) {
      options.toReturn = 'value';
    }
    else {
      options.toReturn = 'ids';
    }
  }
  
  var callback = options.callback || undefined;

  if (!callback) {
    log.debug('runView: no callback!');
  }

  if (options.toReturn === 'ids') {
    viewOptions.include_docs = false;
  }
  else if (viewOptions.reduce) {
    delete options.fetchDocs;
    delete viewOptions.include_docs;
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
                  if (viewOptions.reduce) {
                    docsOrIds = body.rows[0].value;
                  }
                  else if ((options.toReturn === 'docs') && viewOptions.include_docs) {
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
                if (_.isString(body)) {
                  log.error('runView: View return string body - ' + body);
                }
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
}

//
// convert_couch_body_to_array_of_images: Maps the body.rows collection
//  into the proper Array of Image originals and their variants.
//
//  Note, the view could have been scanned in ascending or descending order:
//
//    ascending order (oldest images first): Original will preceed thumnails.
//    descending order (news first): thumbnails will proceed original.
//
function convert_couch_body_to_array_of_images(opts,resultDocs) {

  opts = opts || {};

  var aryImgOut = [];
  var imgMap    = {}; // temporary hashmap that stores original images by oid
  var anImg     = {};
  //
  // orphanedVariants: Required for when the view is traversed in descending
  //  order where thumbnails will come before the original.
  //
  var orphanedVariants = {};

  for (var i = 0; i < resultDocs.length; i++) {
    var docBody = resultDocs[i];

    anImg = mmStorage.docFactory('plm.Image', docBody);
    if (opts.showMetadata) { anImg.exposeRawMetadata = true; }

    // Assign a URL to the image. Note, this is temporary as the images
    // will eventually move out of Couch / Touch DB.
    anImg.url = getImageUrl(docBody);

    if ( anImg.isOriginal()) {
      log.debug('Adding image to result set, id - ' + anImg.oid);
      imgMap[anImg.oid] = anImg;
      aryImgOut.push(anImg);
      if (_.has(orphanedVariants, anImg.oid)) {
        imgMap[anImg.oid].variants.push.apply(imgMap[anImg.oid].variants, orphanedVariants[anImg.oid]);
        delete orphanedVariants[anImg.oid];
      }
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
        if (!_.has(orphanedVariants, anImg.orig_id)) {
          orphanedVariants[anImg.orig_id] = [];
        }
        orphanedVariants[anImg.orig_id].push(anImg);
      }
    }
  }

  return aryImgOut;

}

/*
 *  getImageUrl: Helper to construct a URL to reference the image associated with a document.
 */
function getImageUrl(doc) {
  var docUrl = doc.url ? doc.url : null;
  var url = 'http://' + priv.host;
  if (priv.port) {
    url = url + ':' + priv.port;
  }
  url = url + '/';
  if (priv.database) {
    url = url + priv.database + '/';
  }
  else {
    return docUrl;
  }
  if (doc._id) {
    url = url + doc._id + '/';
  }
  else {
    return docUrl;
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
    return docUrl;
  }
};

module.exports = function(host, port, database) {
  log.info(logPrefix + 'Initializing, host - ' + host + ', port - ' + port + ', database - ' + database);
  priv.host = host;
  priv.port = port;
  priv.database = database;

  return {
    bulkDocFetch: bulkDocFetch,
    fetchDocs: fetchDocs,
    runView: runView,
    iterateOverView: touchdb.iterateOverView,
    iterateOverViewKeys: touchdb.iterateOverViewKeys,
    convert_couch_body_to_array_of_images: convert_couch_body_to_array_of_images,
    getImageUrl: getImageUrl
  };
};
