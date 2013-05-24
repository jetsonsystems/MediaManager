//
// MediaManagerRestClient, version 0.0.
//
//  Client side MediaManager REST API helper library. Allows us to directly
//  call into MediaManagerApiCore without making AJAX requests which go
//  through all the http like routing.
//

var moduleName = './lib/MediaManagerRestClient';

var _ = require('underscore');
var log4js = require('log4js');
var log = log4js.getLogger("plm.MediaManagerRestClient");


//
// We only allow instantiating one of these, which initially MUST be instantiated
// from the node side of things, such that we can pass in an 'app' instance.
//
var restClient = null;

module.exports = function restClientModule(app) {

  if (restClient !== null) {
    //
    //  Not the first time.
    //
    if (app && !_.isEqual(restClient.app, app)) {
      throw Object.create(new Error(),
                          { name: { value: 'ReInstantiatingWithDifferentApp' },
                            message: { value: 'MediaManagerApi/lib/MediaManagerRestClient.js: Reinstantiating with a different app.' } });
    }
    log.info(moduleName + ': returning existing rest client...');
    return restClient;
  }

  if (!app) {
    throw Object.create(new Error(),
                        { name: { value: 'NoApp' },
                          message: { value: 'MediaManagerApi/lib/MediaManagerRestClient.js: app is required.' }
                        });
  }

  restClient = {};

  restClient.VERSION = '0.0';

  restClient.app = app;

  var router = app.mediaManagerRouter;

  log.info(moduleName + ': app - ' + restClient.app.toString() + ', router - ' + router.toString() + ', resources - ' + _.keys(router.resources));

  //
  // Public API methods to access resources:
  //

  //
  // RESOURCE: Images
  //

  restClient.images = {};

  //
  // imagesIndex - index a collection of images.
  //
  restClient.images.index = function() {};

  //
  // imagesGet - Get meta-data about an image.
  //
  restClient.images.get = function() {};

  //
  // imagesUpdate - Update an image.
  //
  restClient.images.update = function(payload) {};

  //
  // imagesDelete - Delete an image.
  //
  restClient.images.delete = function() {};

  //
  // importersCreate - Create an importer.
  //

  restClient.importers = {};

  restClient.importers.create = function(payload) {
    var logPrefix =  moduleName + '.importers.create: ';

    log.info(logPrefix + 'Invoking importers.create w/ payload - ' + payload);

    var attr = {};

    var parsedP = JSON.parse(payload);

    if (_.has(parsedP, "import_dir")) {
      attr["import_dir"] = parsedP.import_dir;
    }

    var onSuccess = function(body) {
      log.info(logPrefix + 'Success - ' + JSON.stringify(body));
    };

    var onError = function(body) {
      log.info(logPrefix + 'Error - ' + JSON.stringify(body));
    };

    router.resources.Importers.create(attr, 
                                      {
                                        onSuccess: onSuccess,
                                        onError: onError
                                      });
    log.info(logPrefix + 'Invoked importers.create!');
  };

  return restClient;

};
