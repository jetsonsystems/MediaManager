'use strict';

var async = require("async")
    ,imageService = require('../lib/plm-image/ImageService')
    ,nano = require('nano')
    ,util = require('util')
    ,updateDesignDoc = require('./update_design_doc');

var chai = require('chai')
    , expect = chai.expect
    , should = require("should");




/**
 * "describe" function is a container for test cases
 * The functions before and after would be called at the beginning and end of executing a describe block,
 * while beforeEach and afterEach are called before and after each test case of a describe block.
 */
describe('ImageService Testing', function () {

    imageService.config.db.host = "localhost";
    imageService.config.db.port = 5984;
    var server = nano('http://' + imageService.config.db.host + ':' + imageService.config.db.port);

    var db_name = imageService.config.db.name = 'plm-media-manager-dev0';

    var options = {
        host:imageService.config.db.host,
        port:imageService.config.db.port,
        dbName:db_name,
        design_doc:'couchdb'
    };

    var db = null;

    //This will be called before all tests
    before(function (done) {

        async.waterfall([

                function checkIfTestDatabaseExists(callback) {
                var existsTestDatabase = false;
                //check if the testing database exists by getting information about database
                server.db.get(db_name, function (err, body) {
                        if (!err) {
                            existsTestDatabase = true;
                        }
                        callback(null, existsTestDatabase);
                    }
                );
            },

            function deleteTestDatabase(existsTestDatabase, callback) {
                if (existsTestDatabase) {
                    console.log('Attempting to destroy existing test database: ' + db_name);
                    server.db.destroy(db_name, function (err, body) {
                        if (!err) {
                            console.log('Existing test database ' + db_name + '  was destroyed');
                        }
                        callback();
                    });

                } else {
                    callback();
                }
            },

            function createTestDatabase(callback) {
                server.db.create(db_name, function (err, body) {
                    if (!err) {
                        console.log('database ' + imageService.config.db.name + '  created!');
                    }
                    callback(null);

                });

            },
            function insertDesignDocs(callback) {

                updateDesignDoc.updateDesignDoc(options,function(err,result){
                        if(err){
                            console.log(err);
                        }
                        else{
                            console.log("Updated Design Doc: " + util.inspect(result,true,null,true));
                        }
                        callback(null);
                    }

                );
            }

        ], function (err, results) {
            db = server.use(db_name);
            // console.log("done with test suite setup");
            done();
        });
    });//end before

    describe('ImageService.save', function () {

        /*
         Test1 Setup:
         - pick an image path from 'test/resources/images'
         - create an image record in couch using ImageService.save(imagePath)
         - perform various 'Assertions' on the Image object returned by
         save(imagePath), comparing the values of the object to a pre-determined set of
         values in the test (size, format, checksum, oid present, etc...)
         *
         *
         */
        var path_to_images = './test/resources/images';

        var theSavedImage = null;

        before(function (done) {

            // simple save
            imageService.save(
                path_to_images + '/clooney.png',
                function(err, result) {
                    // if (err) { console.log(err); throw err; }
                    if (err) {
                        console.log(err);
                        process.exit(1);
                    }
                    console.log("result: " + JSON.stringify(result));
                    // console.log("inspect: " + util.inspect(result));
                    theSavedImage = result;
                    done();
                }
            );

        });//end before

        //Define the tests

        /**
         * Each "it" function is a test case
         * The done parameter indicates that the test is asynchronous
         */
        it("The saved image should have some properties", function (done) {

            util.inspect(theSavedImage,true,null,true);



            theSavedImage.name.should.equal('clooney.png');
            theSavedImage.class_name.should.equal('plm.Image');
            theSavedImage.filesize.should.equal('486.3K');
            theSavedImage.format.should.equal('PNG');
            theSavedImage.size.width.should.equal(480);
            theSavedImage.size.height.should.equal(599);
            theSavedImage.geometry.should.equal("480x599");
            expect(theSavedImage.oid).to.be.not.empty;
            expect(theSavedImage.checksum).to.equal("7f69b43f4ef1ff0933b93e14f702bdac");
            theSavedImage._attachments["clooney.png"].content_type.should.equal('image/PNG');
            theSavedImage._attachments["clooney.png"].stub.should.equal(true);
            expect(theSavedImage.variants).to.be.instanceof(Array);
            expect(theSavedImage.variants).to.be.empty;
            expect(theSavedImage.batch_id).to.be.empty;
            expect(theSavedImage.type).to.be.empty;

            var url = util.format("http://%s:%s/%s/%s/clooney.png", 
                imageService.config.db.host, imageService.config.db.port, imageService.config.db.name, theSavedImage.oid);
            theSavedImage.url.should.equal(url);

            done();
        });

    });
    /**
     * after would be called at the end of executing a describe block, when all tests finished
     */
    after(function (done) {

        async.waterfall([

            function destroyTestDatabase(callback) {
                server.db.destroy(db_name, function (err, body) {
                    if (!err) {
                        console.log('database ' + imageService.config.db.name + '  destroyed!');
                    }
                    callback(null);

                });

            }
        ], function (err, results) {
            done();
        });
    });//end after


});
