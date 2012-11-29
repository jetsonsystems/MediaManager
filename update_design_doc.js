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
      "map" : "function(doc) { if (doc.class_name === 'plm.Image') { var key; if (doc.orig_id === ''){ key = [doc.oid,0,doc.size.width]; } else { key = [doc.orig_id,1,doc.size.width]} emit(key, doc.path)} }"
    },
    "by_creation_time" : { "map" : "function(doc) { function date_to_array(aDate) { var out = [], d = new Date(aDate); out.push(d.getFullYear()); out.push(d.getMonth()+1); out.push(d.getDate()); out.push(d.getHours()); out.push(d.getMinutes()); out.push(d.getSeconds()); out.push(d.getMilliseconds()); return out;} if (doc.class_name === 'plm.Image') { var key = date_to_array(doc.created_at); if (doc.orig_id === '') { key.push(doc.oid,0,doc.size.width);} else { key.push(doc.orig_id,1,doc.size.width); } emit(key,doc.path)  }}"
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
