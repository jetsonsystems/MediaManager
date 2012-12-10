var
    should = require("should")
    , image_util = require("../lib/plm-image/image_util");

describe('fitToSize tests:', function () {

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


describe('fitToSquare tests:', function () {


    it("given size 300x150 and numPix of 100 fitToSquare should give size of 100x50", function () {
        var currentSize = {width:300, height:150};
        var numPix = 100;
        var newSize = image_util.fitToSquare(currentSize, numPix);
        newSize.width.should.equal(100);
        newSize.height.should.equal(50);
    });

    it("given size 150x300 and numPix of 100 fitToSquare should give size of 50x100", function () {
        var currentSize = {width:150, height:300};
        var numPix = 100;
        var newSize = image_util.fitToSquare(currentSize, numPix);
        newSize.width.should.equal(50);
        newSize.height.should.equal(100);
    });

});

describe('fitToHeight tests:', function () {


    it("given size 300x150 and numPix of 100 fitToHeight should give size of 200x100", function () {
        var currentSize = {width:300, height:150};
        var numPix = 100;
        var newSize = image_util.fitToHeight(currentSize, numPix);
        newSize.width.should.equal(200);
        newSize.height.should.equal(100);
    });

    it("given size 150x300 and numPix of 100 fitToHeight should give size of 50x100", function () {
        var currentSize = {width:150, height:300};
        var numPix = 100;
        var newSize = image_util.fitToHeight(currentSize, numPix);
        newSize.width.should.equal(50);
        newSize.height.should.equal(100);
    });

});

describe('fitToWidth tests:', function () {


    it("given size 300x150 and numPix of 100 fitToWidth should give size of 100x50", function () {
        var currentSize = {width:300, height:150};
        var numPix = 100;
        var newSize = image_util.fitToWidth(currentSize, numPix);
        newSize.width.should.equal(100);
        newSize.height.should.equal(50);
    });

    it("given size 150x300 and numPix of 100 fitToWidth should give size of 100x200", function () {
        var currentSize = {width:150, height:300};
        var numPix = 100;
        var newSize = image_util.fitToWidth(currentSize, numPix);
        newSize.width.should.equal(100);
        newSize.height.should.equal(200);
    });

});

describe('aspectRatio tests:', function () {


    it("given size 300x150 aspectRatio should be equal to 2", function () {
        var currentSize = {width:300, height:150};
        var aspectRatio = image_util.aspectRatio(currentSize);
        aspectRatio.should.equal(2);
    });

    it("given size 150x300  aspectRatio should be equal to 0.5", function () {
        var currentSize = {width:150, height:300};
        var aspectRatio = image_util.aspectRatio(currentSize);
        aspectRatio.should.equal(0.5);
    });

});
