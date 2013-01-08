var
  should  = require('should')
  ,expect = require('chai').expect
  ,fs     = require('fs')
  ,Image  = require('../lib/plm-image/Image')
;

describe('Image', function () {

  var 
    metadata = JSON.parse(fs.readFileSync('./test/resources/json/gm_jpg_metadata.json'))
    ,NAME = 'anImage.jpg'
    ,PATH = '/some/path/to/' + NAME
    ,OID  = 'aaa-bbb-ccc'
    ,image = new Image({path: PATH, oid: OID})
  ;

  it("should have default values at initialization", function (done) 
  {
    // console.log("testing default values");
    image.name.should.equal(NAME);
    image.path.should.equal(PATH);
    image.oid.should.equal(OID);
    (image.created_at instanceof Date).should.be.true;
    (image.updated_at instanceof Date).should.be.true;
    image.created_at.should.equal(image.updated_at);
    image.format.should.equal("");
    image.geometry.should.equal("");
    expect(image.size).to.be.empty;
    image.filesize.should.equal("");
    image.variants.should.have.length(0);
    expect(image.metadata_raw).to.be.empty;
    done(); // this ensures that this test completes before the next one executes
  });

  it("should have the data parsed by GraphicsMagic", function () 
  {
    console.log("testing parsed values");
    image.readFromGraphicsMagick(metadata);
    image.name.should.equal(NAME);
    image.path.should.equal(PATH);
    image.oid.should.equal(OID);
    image.format.should.equal("JPEG");
    image.geometry.should.equal("1472x1104");
    image.size.width.should.equal(1472);
    image.size.height.should.equal(1104);
    image.filesize.should.equal("389.6K");
    image.variants.should.have.length(0);
    image.metadata_raw.should.equal(metadata);
  });

});

