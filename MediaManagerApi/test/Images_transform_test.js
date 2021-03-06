'use strict';

var
  should  = require('should')
  ,config = require('config')
  ,expect = require('chai').expect
  ,fs     = require('fs');
var mmStorage  = require('MediaManagerStorage')(config.db);
var Images = require('../lib/MediaManagerApiCore')(config).Images;
var util   = require('util');
;

/** utility function that is used in several tests below */
function assertShortForm(rep, image) 
{
  rep.id.should.equal('$' + image.oid);
  rep.name.should.equal(image.name);
  rep.url.should.equal(image.url);
  rep.geometry.should.equal(image.geometry);
  rep.size.should.equal(image.size);
  rep.filesize.should.equal(image.filesize);
  rep.created_at.should.equal(image.created_at);
  rep.taken_at.should.equal(image.taken_at);
};
exports.assertShortForm = assertShortForm;

describe('Images', function () {

  var 
    URL_VERSION = '/v0'
    ,NAME = 'images'
    ,PATH = '/' + NAME
    ,INST_NAME = 'image'
    ,images   = new Images(PATH, {instName: INST_NAME, pathPrefix: URL_VERSION})
  ;

  var
    IMG_NAME = 'anImage.jpg'
    ,IMG_PATH = '/some/path/to/' + IMG_NAME
    ,IMG_OID  = 'aaa-bbb-ccc'
    ,IMG_METADATA = JSON.parse(fs.readFileSync('./test/resources/json/gm_jpg_metadata.json'))
    ,image = mmStorage.docFactory('plm.Image', {path: IMG_PATH, oid: IMG_OID})
  ;

  image.readFromGraphicsMagick(IMG_METADATA);
  image.checksum = 'SOME_CHECKSUM';
  image.taken_at = new Date();
  image.url = 'http://localhost:5984/some_db/' + IMG_OID;


  it("should have proper values at initialization", function ()
  {
    // console.log(util.inspect(images,true));
    images.name.should.equal(NAME);
    images.path.should.equal(PATH);
    images.instName.should.equal(INST_NAME);
    images.pathPrefix.should.equal(URL_VERSION);
    should.not.exist(images.subResource);
  });


  it("should transform an image to short form via Images.transformRep", function () 
  {
    var rep = images.transformRep([image])[0];
    assertShortForm(rep, image);

    should.not.exist(rep.path);
    should.not.exist(rep.format);
    should.not.exist(rep.checksum);
  });


  it("should transform an image to long form via Images.transformRep", function () 
  {
    var rep = images.transformRep(image, {isInstRef: true});
    assertShortForm(rep, image);

    rep.path.should.equal(image.path);
    rep.format.should.equal(image.format);
    rep.checksum.should.equal(image.checksum);

    // these are not implemented
    // rep.disposition.should.equal(image.disposition);
    // rep.import_root_dir.should.equal(image.import_root_dir);
  });

});
