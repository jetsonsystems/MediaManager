'use_strict';

var 
  nano = require('nano')
  ,async = require('async')
  ,util = require('util')
;

var db = nano('http://localhost:5984/plm_staging');

var design_doc_id = '_design/plm-image';

var design_doc = {
  "views" : {
    "by_oid_with_variant" : {
      "map" : "function(doc) { if (doc.class_name === 'plm.Image') { if (doc.orig_id === ''){ emit([doc._id,0,doc.size.width], doc.path) } else {emit([doc.orig_id,1,doc.size.width],null)}} }"
    },
    "by_creation_time" : { "map" : "function(doc) { if (doc.class_name === 'plm.Image') { emit(doc.created_at, doc.path); }}"
    }
  }
}
/*
*/

async.waterfall(
  [
    function(next) {
      db.get(design_doc_id, next); 
    },
    function(doc, hdr, next) {
      console.log('design doc: %j', doc);
      console.log('doc._rev: %j', doc._rev);
      db.insert(design_doc, {doc_name: design_doc_id, rev: doc._rev}, next);
      // db.insert(design_doc, {rev: doc._rev}, next);
    }
  ],
  function(err, doc, hdr) {
    if (err) console.log('error %j',err);
    console.log("doc: %j", doc);
    console.log("hdr: %j", hdr);
    console.log("all done");
  }
);
  

/*
*/
