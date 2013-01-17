'use strict';

var
  should  = require('should')
  ,expect = require('chai').expect
;

var
  fs     = require('fs')
  ,Image = require('../lib/plm-image/Image')
  ,ImportBatch = require('../lib/plm-image/ImportBatch')
  ,util   = require('util')
;


describe('ImportBatch', function () {

  var
    IMPORT_DIR = '/some/image/dir'
    ,OID  = 'xxx-yyy-zzz'
    ,PATH1 = IMPORT_DIR + '/image1.jpg'
    ,PATH2 = IMPORT_DIR + '/image2.jpg'
    ,IMAGES_TO_IMPORT = [ PATH1, PATH2]
    ,CREATED_AT   = new Date()
    ,STARTED_AT   = new Date( CREATED_AT.getTime() + 1000) 
    ,COMPLETED_AT = new Date( STARTED_AT.getTime() + 5000)
    ,importBatch  = new ImportBatch(
      {path: IMPORT_DIR, oid: OID, images_to_import: IMAGES_TO_IMPORT, created_at: CREATED_AT})
    ,IMG1 = new Image({path: PATH1, oid: OID+'1'})
    ,ERR = "Something bad happened"
    ,out = {}
  ;


  importBatch.setStatus(importBatch.BATCH_INIT);

  // verify the events that will be emitted by the tests below
  importBatch.once(importBatch.event.STARTED,   
    function(anEvent) {
      // console.log("importBatch triggered event: %s - %s", importBatch.event.STARTED, util.inspect(anEvent));
      anEvent.emitted_at.getTime().should.be.above(CREATED_AT.getTime());
      anEvent.data.class_name.should.equal(importBatch.class_name);
      assertStarted(anEvent.data);
    }
  );

  importBatch.once(importBatch.event.IMG_SAVE,  
    function(anEvent) {
      // console.log("importBatch triggered event: %s - %s", importBatch.event.IMG_SAVE, util.inspect(anEvent));
      var img = anEvent.data;
      img.class_name.should.equal(IMG1.class_name);
      img.oid.should.equal(OID+'1');
      img.path.should.equal(PATH1);
    }
  );

  importBatch.once(importBatch.event.IMG_ERROR,
    function(anEvent) {
      // console.log("importBatch triggered event: %s - %s", importBatch.event.IMG_ERROR, util.inspect(anEvent));
      anEvent.data.path.should.equal(PATH2);
      anEvent.data.error.should.equal(ERR);
    }
  );

  importBatch.once(importBatch.event.COMPLETED,
    function(anEvent) {
      // console.log("importBatch triggered event: %s - %s", importBatch.event.COMPLETED, util.inspect(anEvent));
      anEvent.data.class_name.should.equal(importBatch.class_name);
      assertCompleted(anEvent.data);
    }
  );

  /*
  importBatch.once(importBatch.event.IMG_ERROR, assertImageError);
  importBatch.once(importBatch.event.COMPLETED, assertImageError);
  */

  it("should have proper values at initialization", function (done)
  {
    /*
    console.log(util.inspect(importBatch,true)+"\n");
    console.log(importBatch.toJSON());
    */

    importBatch.class_name.should.equal('plm.ImportBatch');
    importBatch.oid.should.equal(OID);
    importBatch.getNumToImport().should.equal(IMAGES_TO_IMPORT.length);
    importBatch.getNumAttempted().should.equal(0);
    importBatch.getNumSuccess().should.equal(0);
    importBatch.getNumError().should.equal(0);
    importBatch.getCreatedAt().should.equal(CREATED_AT);
    importBatch.getUpdatedAt().should.equal(CREATED_AT);
    should.not.exist(importBatch.getStartedAt());
    should.not.exist(importBatch.getCompletedAt());
    importBatch.getStatus().should.equal(importBatch.BATCH_INIT);

    var out = importBatch.toJSON();
    out.class_name.should.equal('plm.ImportBatch');
    out.oid.should.equal(OID);
    out.num_to_import.should.equal(IMAGES_TO_IMPORT.length);
    out.num_attempted.should.equal(0);
    out.num_success.should.equal(0);
    out.num_error.should.equal(0);
    out.created_at.should.equal(CREATED_AT);
    out.updated_at.should.equal(CREATED_AT);
    should.not.exist(out.started_at);
    should.not.exist(out.completed_at);
    out.status.should.equal(importBatch.BATCH_INIT);

    done();
  });

  it("should have proper values after it's started", function (done)
  {
    importBatch.setStartedAt(STARTED_AT);

    /*
    console.log(util.inspect(importBatch,true)+"\n");
    console.log(importBatch.toJSON());
    */

    assertStarted(importBatch);

    done();
  });

  it("should have proper values after an image is added", function (done)
  {
    importBatch.addSuccess(IMG1);

    /*
    console.log(util.inspect(importBatch,true)+"\n");
    console.log(importBatch.toJSON());
    */

    importBatch._proc.images[PATH1].oid.should.equal(OID+'1');
    importBatch._proc.images[PATH1].path.should.equal(PATH1);
    importBatch.getNumToImport().should.equal(IMAGES_TO_IMPORT.length);
    importBatch.getNumAttempted().should.equal(1);
    importBatch.getNumSuccess().should.equal(1);
    importBatch.getNumError().should.equal(0);
    importBatch.getCreatedAt().should.equal(CREATED_AT);
    importBatch.getUpdatedAt().should.equal(CREATED_AT);
    importBatch.getStartedAt().should.equal(STARTED_AT);
    should.not.exist(importBatch.getCompletedAt());
    importBatch.getStatus().should.equal(importBatch.BATCH_STARTED);

    var out = importBatch.toJSON();
    out.class_name.should.equal('plm.ImportBatch');
    out.oid.should.equal(OID);
    out.num_to_import.should.equal(IMAGES_TO_IMPORT.length);
    out.num_attempted.should.equal(1);
    out.num_success.should.equal(1);
    out.num_error.should.equal(0);
    out.created_at.should.equal(CREATED_AT);
    out.updated_at.should.equal(CREATED_AT);
    out.started_at.should.equal(STARTED_AT);
    should.not.exist(out.completed_at);
    out.status.should.equal(importBatch.BATCH_STARTED);

    done();
  });

  it("should have proper values after an error occurs", function (done)
  {
    importBatch.addErr(PATH2, ERR);

    /*
    console.log(util.inspect(importBatch,true)+"\n");
    console.log(importBatch.toJSON());
    */

    importBatch._proc.errs[PATH2].should.equal(ERR);
    importBatch.getNumToImport().should.equal(IMAGES_TO_IMPORT.length);
    importBatch.getNumAttempted().should.equal(2);
    importBatch.getNumSuccess().should.equal(1);
    importBatch.getNumError().should.equal(1);
    importBatch.getCreatedAt().should.equal(CREATED_AT);
    importBatch.getUpdatedAt().should.equal(CREATED_AT);
    importBatch.getStartedAt().should.equal(STARTED_AT);
    should.not.exist(importBatch.getCompletedAt());
    importBatch.getStatus().should.equal(importBatch.BATCH_STARTED);

    var out = importBatch.toJSON();
    out.class_name.should.equal('plm.ImportBatch');
    out.oid.should.equal(OID);
    out.num_to_import.should.equal(IMAGES_TO_IMPORT.length);
    out.num_attempted.should.equal(2);
    out.num_success.should.equal(1);
    out.num_error.should.equal(1);
    out.created_at.should.equal(CREATED_AT);
    out.updated_at.should.equal(CREATED_AT);
    out.started_at.should.equal(STARTED_AT);
    should.not.exist(out.completed_at);
    out.status.should.equal(importBatch.BATCH_STARTED);

    done();
  });

  it("should have proper values after it completes", function (done)
  {
    importBatch.setCompletedAt(COMPLETED_AT);
    assertCompleted(importBatch);

    done();
  });


  /** helper function that gets executed on an event, and during a test */
  function assertStarted(anImportBatch) 
  {
    /*
    console.log(util.inspect(importBatch,true)+"\n");
    console.log(importBatch.toJSON());
    */

    // console.log("assertStarted");

    anImportBatch.getNumToImport().should.equal(IMAGES_TO_IMPORT.length);
    anImportBatch.getNumAttempted().should.equal(0);
    anImportBatch.getNumSuccess().should.equal(0);
    anImportBatch.getNumError().should.equal(0);
    anImportBatch.getCreatedAt().should.equal(CREATED_AT);
    anImportBatch.getUpdatedAt().should.equal(CREATED_AT); // updated_at gets updated on a save to persistent storage
    anImportBatch.getStartedAt().should.equal(STARTED_AT);
    should.not.exist(anImportBatch.getCompletedAt());
    anImportBatch.getStatus().should.equal(anImportBatch.BATCH_STARTED);

    var out = anImportBatch.toJSON();
    out.class_name.should.equal('plm.ImportBatch');
    out.oid.should.equal(OID);
    out.num_to_import.should.equal(IMAGES_TO_IMPORT.length);
    out.num_attempted.should.equal(0);
    out.num_success.should.equal(0);
    out.num_error.should.equal(0);
    out.created_at.should.equal(CREATED_AT);
    out.updated_at.should.equal(CREATED_AT);
    out.started_at.should.equal(STARTED_AT);
    should.not.exist(out.completed_at);
    out.status.should.equal(anImportBatch.BATCH_STARTED);
  }

  function assertCompleted(anImportBatch) 
  {
    /*
    console.log(util.inspect(importBatch,true)+"\n");
    console.log(importBatch.toJSON());
    */
    // console.log("assertCompleted");

    anImportBatch.getNumToImport().should.equal(IMAGES_TO_IMPORT.length);
    anImportBatch.getNumAttempted().should.equal(2);
    anImportBatch.getNumSuccess().should.equal(1);
    anImportBatch.getNumError().should.equal(1);
    anImportBatch.getCreatedAt().should.equal(CREATED_AT);
    anImportBatch.getUpdatedAt().should.equal(CREATED_AT); // updated_at gets updated on a save to persistent storage
    anImportBatch.getStartedAt().should.equal(STARTED_AT);
    anImportBatch.getCompletedAt().should.equal(COMPLETED_AT);
    anImportBatch.getStatus().should.equal(anImportBatch.BATCH_COMPLETED);

    var out = anImportBatch.toJSON();
    out.class_name.should.equal('plm.ImportBatch');
    out.oid.should.equal(OID);
    out.num_to_import.should.equal(IMAGES_TO_IMPORT.length);
    out.num_attempted.should.equal(2);
    out.num_success.should.equal(1);
    out.num_error.should.equal(1);
    out.created_at.should.equal(CREATED_AT);
    out.updated_at.should.equal(CREATED_AT);
    out.started_at.should.equal(STARTED_AT);
    out.completed_at.should.equal(COMPLETED_AT);
    out.status.should.equal(anImportBatch.BATCH_COMPLETED);
  }

});
