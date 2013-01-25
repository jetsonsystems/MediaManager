'use strict';

var
  should  = require('should')
  ,config = require('./config')
  ,expect = require('chai').expect
  ,fs     = require('fs')
  ,ImportBatch = require('ImageService/lib/plm-image/ImportBatch')
  ,Image       = require('ImageService/lib/plm-image/Image')
  ,mmApi      = require('../lib/MediaManagerApiCore')(config, {singleton: false})
  ,Images      = mmApi.Images
  ,Importers   = mmApi.Importers
  ,ImportersImages = mmApi.ImportersImages
  ,ImagesTest = require('./Images_transform_test')
  ,util   = require('util')
;

describe('Importers', function () {

  var 
    URL_VERSION = '/v0'
    ,NAME = 'importers'
    ,PATH = '/' + NAME
    ,INST_NAME = 'importer'
    ,IMG_SUB_RC = 'images'
    ,IMG_INST_NAME = 'image'
    ,resource   = new ImportersImages(null, {
      pathPrefix: URL_VERSION
      ,subResource: new Importers(PATH, {
        instName: INST_NAME 
        ,subResource: new Images('/' + IMG_SUB_RC, {instName: IMG_INST_NAME})
      })
    })
  ;

  it("should have proper values at initialization", function ()
  {
    // console.log(util.inspect(resource,true));
    resource.pathPrefix.should.equal(URL_VERSION);
    should.not.exist(resource.path);
    resource.name.should.equal('');
    resource.instName.should.equal('');

    var sub1 = resource.subResource;
    sub1.pathPrefix.should.equal('');
    sub1.path.should.equal(PATH);
    sub1.name.should.equal(NAME);
    sub1.instName.should.equal(INST_NAME);

    var sub2 = sub1.subResource;
    sub2.pathPrefix.should.equal('');
    sub2.path.should.equal('/' + IMG_SUB_RC);
    sub2.name.should.equal(IMG_SUB_RC);
    sub2.instName.should.equal(IMG_INST_NAME);
    should.not.exist(sub2.subResource);
  });

  var
    TEST_DIR = './test/resources/json'
    ,IMPORT_DIR = '/some/image/dir'
    ,OID  = 'xxx-yyy-zzz'
    ,importBatch = new ImportBatch({path: IMPORT_DIR, oid: OID})
    ,img1 = new Image({path: IMPORT_DIR + '/img1.jpg', oid: OID + '-1', batch_id: OID})
    ,img2 = new Image({path: IMPORT_DIR + '/img2.png', oid: OID + '-2', batch_id: OID})
  ;

  img1.readFromGraphicsMagick(JSON.parse(fs.readFileSync(TEST_DIR + '/gm_jpg_metadata.json')));
  img2.readFromGraphicsMagick(JSON.parse(fs.readFileSync(TEST_DIR + '/gm_png_metadata.json')));

  img1.taken_at = new Date();
  img2.taken_at = new Date();

  img1.url = 'http://localhost:5984/some_db/' + img1.oid;
  img2.url = 'http://localhost:5984/some_db/' + img2.oid;

  importBatch.images.push(img1);
  importBatch.images.push(img2);

  importBatch.num_to_import = 0; // the batch completed, so there is nothing to import
  importBatch.num_imported  = 3; 
  importBatch.num_success   = 2;
  importBatch.num_error     = 1;

  importBatch.setStartedAt( new Date() );
  importBatch.setCompletedAt( new Date(importBatch.getStartedAt().getTime() + 1000) );
  // importBatch.status = 'COMPLETED';

  it("should transform a importBatch with images via ImportersImages.transformRep", function () 
  {
    // console.log("batch: %s", util.inspect(importBatch));
    var rep = resource.transformRep(importBatch);
    // console.log("rep: %s", util.inspect(rep));
    rep.id.should.equal('$' + importBatch.oid);
    rep.created_at.should.equal(importBatch.created_at);
    rep.started_at.should.equal(importBatch.getStartedAt());
    rep.completed_at.should.equal(importBatch.getCompletedAt());
    rep.num_to_import.should.equal(importBatch.getNumToImport());
    rep.num_imported.should.equal(importBatch.getNumAttempted());
    rep.num_success.should.equal(importBatch.getNumSuccess());
    rep.num_error.should.equal(importBatch.getNumError());
    rep.images.length.should.equal(2);
    
    ImagesTest.assertShortForm(rep.images[0], img2);
    ImagesTest.assertShortForm(rep.images[1], img1);
  });

});
