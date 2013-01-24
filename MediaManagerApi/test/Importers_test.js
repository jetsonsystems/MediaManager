'use strict';

var 
  async = require('async')
  ,log4js = require('log4js')
  ,nano = require('nano')
  ,util = require('util')
;

var
  config = require('./config')
  ,dbMan = require('./databaseManager.js')
  ,imageService = require('ImageService')
  ,Importers = require('../lib/MediaManagerApiCore')(config, {singleton: false}).Importers
  ,WebSocket = require('../lib/NotificationsWsLike')
;

var 
  DESTROY_DB_AFTER = true // set to false if you want to see the state of the db after the tests
  ,IMPORT_DIR = './test/resources/images'
  ,NUM_TO_IMPORT = 12
  ,resource = new Importers('/importers', {instName: 'importer', pathPrefix: '/v0'})
;

log4js.configure('./test/log4js.json');

/**
 * "describe" function is a container for test cases
 * The functions before and after would be called at the beginning and end of executing a describe block,
 * while beforeEach and afterEach are called before and after each test case of a describe block.
 */
describe('Importers Resource', function () {
  // set a high time-out because it may take a while to run the import
  this.timeout(180000);  // 3 minutes

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

  function isConnectionEstablished(parsedMsg) {
    return (parsedMsg.resource === '/notifications' && parsedMsg.event === 'connection.established');
  }

  function doSubscriptions(aWs) {
    aWs.send(JSON.stringify({
      "resource": "_client", 
      "event": "subscribe",
      "data": {
        "resource": "/importers" 
      }}));
  }

  //This will be called before all tests
  before(function (done) {
    dbMan.startDatabase(options, function() { done();});
  });//end before


  //Define the test
  it("should have the proper values after it completes", function (done) 
  {
    var opts = {
      attr: { import_dir: IMPORT_DIR }
      ,isInstRef: true
    };

    var counter = {
      started:0
      ,img_saved:0
      ,img_error:0
    };

    var ws = new WebSocket('ws://appjs/notifications');

    console.log('waiting for messages');

    ws.onmessage = function(msg) 
    {
      console.log('Got message: %s', msg.data);
      var parsedMsg = JSON.parse(msg.data);

      if (isConnectionEstablished(parsedMsg)) {
        doSubscriptions(ws);
        resource.doRequest('POST', opts);
      } else {
        switch(parsedMsg.event) {
          case "import.started":
            counter.started++;
            break;
          case "import.image.saved":
            counter.img_saved++;
            break;
          case "import.image.error":
            counter.img_error++;
            break;
          case "import.completed":
            console.log("Received notification of import completion");
            counter.started.should.be.equal(1);
            counter.img_saved.should.be.equal(4);
            counter.img_error.should.be.equal(0);
            done();
            break;
        }
      }
    };
  });

  /**
   * after would be called at the end of executing a describe block, when all tests finished
   */
  after(function (done) {
    if (DESTROY_DB_AFTER) {
      dbMan.destroyDatabase( function() { done(); });
    } else {
      done();
    }

  });//end after

});
