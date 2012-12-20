'use strict';


//TODO: start server

//TODO: do some tests

//TODO: shutdown server

var restify = require('restify')
    ,should = require('should');

// init the test client
var client = restify.createJsonClient({
  version:'*',
  url:'http://127.0.0.1:9000'
});

describe('service: MediaManagerApi', function () {

  //http://localhost:9000/v0/images
  // Test #1
  describe('200 response check', function () {
    it('should get a 200 response', function (done) {
      client.get('/v0/images', function (err, req, res, data) {

        should.not.exist(err);
        res.should.have.status(200);

        done();
      });
    });
  });

  // TODO: Add more tests...


});