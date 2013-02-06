'use strict';

var
  nano = require('nano')
  , async = require('async');


exports.updateDesignDoc = updateDesignDoc;

function updateDesignDoc(options, callback) {

  var db = nano('http://' + options.host + ':' + options.port + '/' + options.dbName);

  console.log('Updating db: host - ' + options.host + ', port - ' + options.port + ', db name - ' + options.dbName);


  var design_doc_id = '_design/plm-image';

  var design_doc_couchdb = {
    "views":{
      "by_oid_with_variant":{
        "map":"function(doc) { if (doc.class_name === 'plm.Image') { var key; if (doc.orig_id === ''){ key = [doc.oid,0,doc.size.width]; emit(key, doc.path) } else { key = [doc.orig_id,1,doc.size.width]; emit(key, doc.name)}} }"
      }, "by_creation_time":{
        "map":"function(doc) { if (doc.class_name === 'plm.Image') { var key = date_to_array(doc.created_at); if (doc.orig_id === '') { key.push(doc.oid,0,doc.size.width); emit(key,doc.path)} else { key.push(doc.orig_id,1,doc.size.width); emit(key,doc.name) }} function date_to_array(aDate) { var out = [], d = new Date(aDate); out.push(d.getFullYear()); out.push(d.getMonth()+1); out.push(d.getDate()); out.push(d.getHours()); out.push(d.getMinutes()); out.push(d.getSeconds()); out.push(d.getMilliseconds()); return out;} }"
      }, "batch_by_ctime":{
        "map":"function(doc) { if (doc.class_name === 'plm.ImportBatch') { var key = date_to_array(doc.created_at); key.push(doc.oid,0); emit(key,doc.path); } function date_to_array(aDate) { var out = [], d = new Date(aDate); out.push(d.getFullYear()); out.push(d.getMonth()+1); out.push(d.getDate()); out.push(d.getHours()); out.push(d.getMinutes()); out.push(d.getSeconds()); out.push(d.getMilliseconds()); return out; }}"
      }, "batch_by_oid_w_image":{
        "map":"function(doc) { var key = []; if (doc.class_name === 'plm.ImportBatch') { key.push(doc.oid,'0',0,0); emit(key,doc.path); } if (doc.class_name === 'plm.Image') { if (doc.orig_id === '') { key.push(doc.batch_id, doc.oid, 1, doc.size.width);} else { key.push(doc.batch_id, doc.orig_id, 2, doc.size.width); } emit(key,doc.name) } function date_to_array(aDate) { var out = [], d = new Date(aDate); out.push(d.getFullYear()); out.push(d.getMonth()+1); out.push(d.getDate()); out.push(d.getHours()); out.push(d.getMinutes()); out.push(d.getSeconds()); out.push(d.getMilliseconds()); return out; }}"
      }, "by_tag" :  {
        "map" : "function(doc) { if (doc.class_name === 'plm.Image'){ if (doc.tags) { for (var tag in doc.tags) {  emit(doc.tags[tag],doc.tags); }  } } }"
      }
    }
  };

  var design_doc_touchdb = {
    "views":{
      "image_by_oid_w_variant":{
      }, "image_by_ctime":{
      }, "batch_by_ctime":{
      }, "batch_by_oid_w_image":{
      }
    }
  };

  var design_doc;

  if (options.design_doc === 'couchdb') {
    console.log('DB type is couchdb');
    design_doc = design_doc_couchdb;
  }
  else {
    console.log('DB type is touchdb');
    design_doc = design_doc_touchdb;
  }


  /*
   */

  var isUpdate = true;

  async.waterfall(
    [
      function (next) {
        console.log("attempting to get design doc with id: %s", design_doc_id);
        db.get(design_doc_id, function (err, doc, hdr) {
          if (err) {
            console.log("design doc does not exist, inserting it...");
            isUpdate = false;
            db.insert(design_doc, design_doc_id, next);
          } else {
            next(null, doc, hdr);
          }
        });
      },
      function (doc, hdr, next) {
        if (isUpdate) {
          console.log('have design doc:', doc);
          console.log('updating doc w/ doc._rev: %s', doc._rev);
          db.insert(design_doc, {doc_name:design_doc_id, rev:doc._rev}, next);
        }
        else {
          next(null, doc, hdr);
        }
      }
    ],
    function (err, result, hdr) {
      if (err) {
        console.log('Unable to insert/update design doc: ', err);
        callback(err);
      }
      console.log("result: %j", result);
      // console.log("hdr: %j", hdr);
      if (isUpdate) {
        console.log("Successfully updated design doc");
      } else {
        console.log("Successfully inserted design doc");
      }
      callback(null, result);
    }
  );
}

/*
 */
