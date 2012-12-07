var
    should = require("should")
    , image_util = require("../lib/plm-image/image_util");

describe('fitToSize tests', function () {

    var currentSize = {width:20, height:40};

    it("given height of 100 fitToSize should give width of 50", function () {
        var proposedNewSize = {height:100};
        var newSize = image_util.fitToSize(currentSize, proposedNewSize);
        newSize.width.should.equal(50);
    });


     it("given width of 50 fitToSize should give height of 100", function () {
     var proposedNewSize = {width:50};
     var newSize = image_util.fitToSize(currentSize, proposedNewSize);
     newSize.height.should.equal(100);
     });


    it("given height and width the size returned by fitToSize should be equal to the given size", function () {
        var proposedNewSize = {width:50, height:100};
        var newSize = image_util.fitToSize(currentSize, proposedNewSize);
        newSize.width.should.equal(proposedNewSize.width);
        newSize.height.should.equal(proposedNewSize.height);
    });

    it("Should throw exception due to malformed size", function () {
            (function () {
                var malformedSize = {width:-20, height:-40};
                var newSize = image_util.fitToSize(currentSize, malformedSize);
            }).should.throw();
        }

    );

});



/*fitToSquare
 fitToHeight
 fitToWidth
 aspectRatio
 */