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

var isUpdate = true;

async.waterfall(
  [
    function(next) {
      console.log("attempting to get design doc with id: %j", design_doc_id);
      db.get(design_doc_id, function(err, doc, hdr) {
        if (err) {
          console.log("design doc does not exist, inserting it...");
          isUpdate = false;
          db.insert(design_doc, design_doc_id, next);
        } else {
          next(null, doc, hdr);
        }
      }); 
    },
    function(doc, hdr, next) {
      if (isUpdate) {
        // console.log('design doc:', doc);
        console.log('doc._rev: %j', doc._rev);
        db.insert(design_doc, {doc_name: design_doc_id, rev: doc._rev}, next);
      }
      else { next(null, doc, hdr); }
    }
  ],
  function(err, result, hdr) {
    if (err) { 
      console.log('Unable to insert/update design doc: ',err); 
      return; 
    }
    console.log("result: %j", result);
    // console.log("hdr: %j", hdr);
    if (isUpdate) {
      console.log("Successfully updated design doc");
    } else {
      console.log("Successfully inserted design doc");
    }
  }
);
  

/*
*/
