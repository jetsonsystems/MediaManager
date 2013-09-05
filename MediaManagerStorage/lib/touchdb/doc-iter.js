var util = require('util');
var _ = require('underscore');
var Q = require('q');
var nano = require('nano');
var log = require('log4js').getLogger("plm.MediaManagerStorage");

var moduleName = './lib/touchdb/doc-iter';

var db = undefined;

//
// DocIterator:
//
//  Args:
//    pageSize
//    designDoc
//    view
//    options
//      startKey: key to start paging at.
//      transform(doc, callback): asyncronous transform function. IE:
//        A request to get images in an import.
//      filterSync(doc): syncronous filter function.
//
function DocIterator(pageSize, designDoc, view, options) {

  var logPrefix = moduleName + '.DocIterator: ';

  options = options || {};

  options.host = options.host || 'localhost';
  options.port = options.port || '59840';

  log.debug(logPrefix + 'Creating doc iterator...');

  var startKey = undefined;
  var startKeyDocId = undefined;
  var done = false;

  //
  // fetchFromDB: Run the view to get docs from TouchDB / CouchDB.
  //
  //  Args:
  //    n - number to get beginning at startKey
  //    callback(err, rows)
  //
  var fetchFromDB = function(n, callback) {

    var lp = logPrefix.replace(': ', '.fetchFromDB: ');

    var view_opts = {
      descending: true,
      include_docs: true
    };
    if (startKey) {
      view_opts.startkey = startKey;
      view_opts.startkey_docid = startKeyDocId;
    }
    if (n) {
      view_opts.limit = n;
    }

    log.debug(lp + 'running view - ' + view + ', w/ view options - ' + util.inspect(view_opts));

    db.view(designDoc, 
            view, 
            view_opts,
            function(err, body) {
              log.debug(lp + 'Got ' + body.rows.length + ' rows...');
              callback(err, body.rows);
            });
  };

  var applyFilter = function(row) {
    if (options.filterSync) {
      if (options.filterSync(row.doc)) {
        row.doc = null;
      }
    }
    return row;
  };

  var transformAndFilterRow = function(row, callback) {
    options.transform(row.doc, function(err, doc) {
      if (err) {
        callback(err);
      }
      else {
        row.doc = doc;
        applyFilter(row);
        callback(null, row);
      }
    });
  };

  var transformAndFilter = function(rows, numToTransform) {

    var lp = logPrefix.replace(': ', '.transform: ');

    var p;

    if (options.transform) {
      var ps = [];

      _.each(rows.slice(0, numToTransform), function(row) {
        // log.debug(lp + 'Adding row to transform...');
        ps.push(Q.nfcall(transformAndFilterRow, row));
      });
      _.each(rows.slice(numToTransform, rows.length), function(row) {
        ps.push(Q.nfcall(function(row, callback) {
          callback(null, row);
        }, row));
      });
      p = Q.all(ps);
    }
    else {

      log.debug(lp + 'No transform required, returning raw rows...');

      p = Q.fcall(function() {
        for (var i = 0; i < numToTransform; i++) {
          applyFilter(rows[i]);
        }
        return rows;
      });
    }
    return p;
  };

  var getPage = function(callback) {

    var lp = logPrefix.replace(': ', 'DocIterator.getPage: ');

    var err = null;
    var page = [];

    var doN = function() {
      var lpp = lp.replace(': ', '.doN: ')
      var nToFetch = pageSize?pageSize+1:undefined;
      var p = Q.nfcall(fetchFromDB, nToFetch);

      log.debug(lpp + 'About to get n - ' + nToFetch);

      p.then(
        function(rows) {
          var numToTransform = 
            ((nToFetch === undefined) || (nToFetch > rows.length)) ?
            rows.length : pageSize;

          return transformAndFilter(rows, numToTransform);
        },
        function(err) {
          callback(err, null);
        }).then(
          function(rows) {
            log.debug(lpp + 'Got ' + rows.length + ' rows after transform / filtering...');
            if (options.filterSync) {
              page = page.concat(_.filter(_.pluck(rows.slice(0, pageSize), 'doc'),
                                          function(doc) {
                                            return doc !== null;
                                          }));
            }
            else {
              page = page.concat(_.pluck(rows.slice(0, pageSize), 'doc'));
            }
            if (!pageSize || (rows.length < nToFetch)) {
              log.debug(lpp + 'Fetched all documents...');
              startKey = undefined;
              startKeyDocId = undefined;
              done = true;
              callback(null, page)
            }
            else {
              log.debug(lpp + 'Fetched page of size - ' + page.length);
              startKey = rows[pageSize].key;
              startKeyDocId = rows[pageSize].id;
              if (page.length < pageSize) {
                doN();
              }
              else {
                log.debug(lpp + 'Invoking callback with page...');
                callback(null, page);
              }
            }
          },
          function(err) {
            callback(err, null);
          });
    };
    doN();
  };

  this.next = function() {
    var lp = logPrefix.replace(': ', '.next: ');
    var p;
    if (done) {
      log.debug(lp + 'Done iterating!');
      p = Q.fcall(function() {
        throw new Error('stop-iteration');
      });
    }
    else {
      log.debug(lp + 'Setting up iteration promise.');
      p = Q.nfcall(getPage);
    }
    return p;
  }

  log.debug(logPrefix + 'Created DocIterator!');

}

exports = module.exports = function(database, host, port) {

  var dbServer = undefined;

  dbServer = nano(
    {
      url: 'http://' + host + ':' + port
    }
  );

  db = dbServer.use(database);

  return {
    Iter: DocIterator
  };
};

//
// Some small functions to demonstrate use...
//

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

var iterateThruBatches = function() {
  var dIt = new DocIterator(
    10, 
    IMG_DESIGN_DOC, 
    VIEW_BATCH_BY_CTIME);

  var docs = [];

  dIt.next().then(
    function(page) {
      console.log('me: got page of length - ' + page.length);
      _.each(page, function(batch) {
        console.log('Got page w/ id - ' + batch.id);
      });
      me();
    },
    function(err) {
      console.log('iteration error - ' + err);
    });
};

var fetchAllBatches = function() {
  var dIt = new DocIterator(
    undefined, 
    IMG_DESIGN_DOC, 
    VIEW_BATCH_BY_CTIME);

  var docs = [];

  dIt.next().then(
    function(page) {
      console.log('fetchAllBatches: got ' + page.length + ' batches...');
    },
    function(err) {
      console.log('fetchAllBatches: err - ' + err);
    });
};