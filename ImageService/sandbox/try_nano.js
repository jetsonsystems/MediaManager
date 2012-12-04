'use strict';


var 
  nano   = require('nano')
  ,db    = nano('http://localhost:5984/plm_staging')
  ,async = require('async')
  ,util  = require('util')
;

async.waterfall([
  function(next) {
    db.get('feb87eb2-9a89-4ec9-a2f1-73ed8c01e8e7', null, function(err, body) {
      // console.log("Displaying an image doc using 'db.get'")
      // console.log(body);
    });
    next();
  },

  function(next) {
    // db.view(design_name, view_name,[params],[callback])
    // assumes a view where the key is [orig_id, [0|1], width]
    // where 0 = original, 1 = variant
    db.view('plm-image', 'by_oid_with_variant', 
      { 
        startkey: ['a060bfea-0845-437e-97bf-989d55d189cb', 0, 0]
        ,endkey:  ['a060bfea-0845-437e-97bf-989d55d189cb', 1, 999999]
        ,include_docs: true
      }, 
      function(err, body) {
        console.log("Displaying an image and its variants using 'db.view'");
        console.log(util.inspect(body));
        if (!err) {
          body.rows.forEach(function(row) {
            console.log('oid %j - size %j - orig_id',row.doc.oid, row.doc.geometry, row.doc.orig_id);
            // console.log(doc);
          });
        } else {
          console.log("err: " +  err);
        };
        next();
      }
  );
  }
], function(err) {
  console.log("done!");
});

