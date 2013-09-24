'use strict';

var
  should  = require('should')
  ,config = require('config')
  ,expect = require('chai').expect
  ,fs     = require('fs');
var mmStorage  = require('MediaManagerStorage')({}, {singleton: false});
var Importers = require('../lib/MediaManagerApiCore')(config, {singleton: false}).Importers;
var util   = require('util');

describe('Importers', function () {

  var 
    URL_VERSION = '/v0'
    ,NAME = 'importers'
    ,INST_NAME = 'importer'
    ,PATH = '/' + NAME
    ,importers   = new Importers(PATH, {instName: INST_NAME, pathPrefix: URL_VERSION})
  ;

  var IMPORT_DIR = '/some/image/dir';
  var OID  = 'xxx-yyy-zzz';
  var IMAGES_TO_IMPORT = [ IMPORT_DIR + '/image1.jpg', IMPORT_DIR + '/image2.jpg'];
  var batchImport = mmStorage.docFactory('plm.ImportBatch', {path: IMPORT_DIR, oid: OID, images_to_import: IMAGES_TO_IMPORT});

  it("should have proper values at initialization", function ()
  {
    // console.log(util.inspect(images,true));
    importers.name.should.equal(NAME);
    importers.path.should.equal(PATH);
    importers.instName.should.equal(INST_NAME);
    importers.pathPrefix.should.equal(URL_VERSION);
    should.not.exist(importers.subResource);
  });


  it("should transform a batchImport via Importers.transformRep", function () 
  {
    batchImport.setStartedAt(new Date());
    var rep = importers.transformRep(batchImport, {isInstRef: true});
    // console.log("batch: %s", util.inspect(batchImport));
    // console.log("rep: %s", util.inspect(rep));
    rep.id.should.equal('$' + batchImport.oid);
    rep.created_at.should.equal(batchImport.created_at);
    rep.started_at.should.equal(batchImport.getStartedAt());
    rep.num_to_import.should.equal(batchImport.getNumToImport());
    rep.num_imported.should.equal(batchImport.getNumAttempted());
    rep.num_success.should.equal(batchImport.getNumSuccess());
    rep.num_error.should.equal(batchImport.getNumError());
    should.not.exist(rep.completed_at);
  });
});
