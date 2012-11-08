//
//  MediaManagerApiCore: Sever side Node.js implemenation of the Media Manager API. This module can
//    be imported into a server and requests routed to the resources and/or methods exposed in this
//    supporting module. 
//
//    The API defines RESTful resources which are exported. See the Resource class below.
//    Other resources such as Images derive from it.
//
//    General Notes:
//
//      * Request / Response payloads: currently all payloads are JSON objects.
//
//      * Common Method Parameters:
//
//        options:
//          onSuccess(responseBody)
//          onError(responseBody)
//

var _ = require('underscore');
require('mootools');
var imageService = require('PLM/ImageService/lib/plm-image/ImageServiceProto0');

imageService.config.db.port = 59840;
imageService.config.db.name = 'plm-media-manager-test0';

var ConsoleLogger = function(debugLevel) {

  this.debugLevel = debugLevel;
  this.module = '/MediaManager/MediaManagerApi/lib/MediaManagerApiCore';

  this.log = function(context, msg) {
    debugLevel <= 0 || console.log(this.module + '.' + context + ': ' + msg);
  }
};

cLogger = new ConsoleLogger(1);

//
//  Resource: base for all RESTful resources.
//      <resource name> = function(method, path, options) { <resource definition> }
//
//    The standard RESTful acctions, can ussually be called explicitly, ie:
//      index, create, read, update, delete.
//
//    doRequest(method, path, options):
//      Perform a request on the resource. Delegates to one of index, create, read, update
//      or delete as appropriate.
//
//        method ::= 'GET' || 'POST' || 'GET' || 'PUT' || 'DELETE'
//        path: Path to resource collection or instance of resource.
//        options: See options above.
//
//      Follows the typical RESTful semantics, ie:
//
//        method  path to                     action
//
//        GET     resource collection         index
//        POST    resource collection         create
//        GET     resource instance           read
//        PUTE    resource instance           update
//        DELETE  resource instance           delete
//
var Resource = new Class({

  index: function(options) { return this; },
  create: function(rep, options) { return this; },
  read: function(id, options) { return this; },
  update: function(id, options) { return this; },
  delete: function(id, options) { return this; },

  //
  //  initialize
  //    path - path to the resource w/o an instance ID.
  //
  initialize: function(path, options) {
    this.path = path;
    this.name = path.replace('/', '');
  },

  doRequest: function(method, path, options) {
    var instId = path.replace(this.path, '');

    if (instId) {
      if (method === 'GET') {
        return this.read(instId, options);
      }
      else if (method === 'PUT') {
        return this.update(instId, options);
      }
      else if (method === 'DELETE') {
        return this.update(instId, options);
      }
    }
    else {
      if (method === 'GET') {
        return this.index(options);
      }
      else if (method === 'POST') {
        return this.create(options.rep, options);
      }
    }
  },

  doCallback: function(status, response, options) {
    var errorCode = options.errorCode || 0;
    var errorMessage = options.errorMessage || null;
    if ((status === 200) && options.onSuccess) {
      options.onSuccess(this.wrapResponseBody(status,
                                              response));
    }
    else if (options.onError) {
      options.onError(this.wrapResponseBody(status,
                                            response,
                                            errorCode,
                                            errorMessage));
    }
  },

  wrapResponseBody: function(status, responseBody, errorCode, errorMessage) {
    var body = {
      status: status,
    };
    if (errorCode) {
      body.error_code = errorCode;
    }
    if (errorMessage) {
      body.error_message = errorMessage;
    }
    if (responseBody) {
      body[this.name] = responseBody;
    }
    return body;
  }

});

//
//  Images Resource:
//
exports.Images = new Class({

  Extends: Resource,

  index: function(options) {
    cLogger.log('Images.index', 'indexing...');
    var that = this;
    imageService.index(function(err, response) {
      var status = 200;
      if (err) {
        cLogger.log('Images.index', 'error from image service - ' + err);
        status = 500;
        options.errorCode = -1;
        options.errorMessage = err;
      }
      cLogger.log('Images.index', 'invoking callback with status - ' + status);
      that.doCallback(status, response, options);
    });
    return that;
  },

  initialize: function(path, options) {
    this.parent(path, options);
  }

});
