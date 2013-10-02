//
//  MediaManagerApiCore: Sever side Node.js implemenation of the Media Manager API. This module can
//    be required into a server and requests routed to the resources and/or methods exposed in this
//    supporting module. 
//
//    Note, to require the module, do as follows:
//
//    var mmApi = require('MediaManagerApiCore')(<your config>, <options>);
//
//      <your config> ::= config as defined in MediaManagerAppConfig. See init() below for
//        backward compatability.
//
//      <options>:
//        singleton: true | false, default is true.
//          By default, a single instance of the API is returned, which is a safer approach
//          when multiple requires may be performed in the context of a single application.
//          However, at times, one may desire multiple instances such as in the context of
//          a test harness.
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
//          onSuccess(responseBody): Callback on success.
//          onError(responseBody): Callback on failure.
//          id: Id of an instance of a resource.
//          query: A parsed query string, using querystring.parse, hopefully.
//          attr: Hash of attributes to pass to a create or update operation.
//          req: The http.ServerRequest object, if someone needs it.
//

var _ = require('underscore');
require('mootools');
var imageService = undefined;
var imageServicePackage = require('ImageService/package.json');
var log4js = require('log4js');
var qs = require('querystring');
var util  = require('util');
var async = require('async');

var log = log4js.getLogger('plm.MediaManagerApi');

//
// Require the storage module. Don't instantiate the module by invoking
// it until we have a proper configuration.
//
var storageModule = require('MediaManagerStorage');
var storageModuleInst = undefined;
var storage = null;

var notifications = require('./Notifications');

//
// mediaManagerApi: Single instance of our module.
//
var mediaManagerApi = null;

//
// mediaManagerApi: The module which is our single export. 
//  Suggested usage is:
//
//    var mmApi = require('MediaManagerApiCore')(<some config>);
//
module.exports = function mediaManagerApiModule(config, options) {
  options = options || {};
  if (!_.has(options, 'singleton')) {
    options.singleton = true;
  }
  if (options.singleton && (mediaManagerApi !== null)) {
    //
    //  Not the first time, you can leave off the config. Otherwise, it must be the same as the previous one.
    //
    if (config && !_.isEqual(mediaManagerApi.config, config)) {
      var msg = 'MediaManagerApi/lib/MediaManagerApiCore.js: Reinstantiating with a different configuration. Module is a singleton.';
      log.error(msg);
      throw Object.create(new Error(msg),
                          { name: { value: 'ReinstantiatingWithDifferentConfig' },
                            message: { value: msg } });
    }
    log.info('mediaManagerApiModule: Returning alreading created module instance!');
    return mediaManagerApi;
  }

  //
  //  Must pass a config. the first time.
  //
  if (!config) {
    var msg = 'MediaManagerApi/lib/MediaManagerApiCore.js: A config. is required to instantiate the module.';
    throw Object.create(new Error(msg),
                        { name: { value: 'NoConfig' },
                          message: { value: msg } });
  }

  init(config, options);

  //
  //  mediaManagerApi: The return object as a result of module instantiation.
  //
  mediaManagerApi = Object.create({}, { 
    config: { value: config },
    Images: { value: Images },
    Tags: { value: Tags },
    Tagger: { value: Tagger },
    Importers: { value: Importers },
    ImportersImages: { value: ImportersImages },
    StorageSynchronizers: { value: StorageSynchronizers },
    StorageChangesFeed: { value: StorageChangesFeed }
  });

  log.info('mediaManagerApiModule: Returning new module instance...');

  return mediaManagerApi;
};

//
// init: init from configuration.
//
//    config: Accepts a config as in MediaManagerAppConfig, and for backward
//      compatability, also the following inividual properties:
//
//      dbHost
//      dbPort
//      dbName
//
var init = function(config, options) {

  log.info('init: Initializing...');

  var useConfig;

  if (_.has(config, 'db')) {
    useConfig = config;
    if (!_.has(config.db, 'local')) {
      useConfig.db.local = {};
    }
  }
  else {
    useConfig = { db: 
                  {
                    local: {},
                    remote: {}
                  }
                };
    if (_.has(config, 'dbHost')) {
      useConfig.db.local.host = config.dbHost;
    }
    if (_.has(config, 'dbPort')) {
      useConfig.db.local.port = config.dbPort;
    }
    if (_.has(config, 'dbName')) {
      useConfig.db.database = config.dbName;
    }
  }

  if (!_.has(useConfig.db.local, 'host')) {
    useConfig.db.local.host = 'localhost';
  }
  if (!_.has(useConfig.db.local, 'port')) {
    useConfig.db.local.port = 59840;
  }
  if (!_.has(useConfig.db, 'database')) {
    useConfig.db.database = 'plm-media-manager';
  }

  //
  // This is a hack, since we need to get a storageModule singleton
  // prior to requiring the ImageService. We should change ImageService
  // to a function invokation with config.
  //
  storageModuleInst = storageModule(config.db, {singleton: options.singleton});
  imageService = require('ImageService');

  if (_.has(useConfig.db, 'local')) {
    if (_.has(useConfig.db.local, 'host')) {
      imageService.config.db.host = useConfig.db.local.host;
    }
    if (_.has(useConfig.db.local, 'port')) {
      imageService.config.db.port = useConfig.db.local.port;
    }
  }
  if (_.has(useConfig.db, 'database')) {
    imageService.config.db.name = useConfig.db.database;
  }
  if (_.has(useConfig, 'app')) {
    imageService.config.app = useConfig.app;
  }
  storage = storageModuleInst.get('touchdb');
};

/*
var ConsoleLogger = function(debugLevel) {

  this.debugLevel = debugLevel;
  this.module = '/MediaManager/MediaManagerApi/lib/MediaManagerApiCore';

  this.log = function(context, msg) {
    this.debugLevel <= 0 || console.log(this.module + '.' + context + ': ' + msg);
    return;
  }
};

var cLogger = new ConsoleLogger(1);
*/

log.info('Using ImageService version - %s', imageServicePackage.version);

//
//  Resource: base for all RESTful resources.
//      <resource name> = function(method, path, options) { <resource definition> }
//
//    The standard RESTful acctions, can ussually be called explicitly, ie:
//      index, create, read, update, delete.
//
//    index(options):
//      Args:
//        * Options: see above.
//
//    create(attr, options):
//      Args:
//        * attr: Hash of attributes to assign to the newly created resource.
//        * Options: see above.
//
//    read(id, options):
//      Args:
//        * id: ID of resource to retrieve.
//        * Options: see above.
//
//    update(id, attr, options):
//      Args:
//        * id: ID of resource to modify.
//        * attr: Hash of attributes to modify.
//        * Options: see above.
//
//    delete(id, options):
//      Args:
//        * id: ID of resource to delete.
//        * Options: see above.
//
//    doRequest(method, options):
//      Perform a request on the resource. Delegates to one of index, create, read, update
//      or delete as appropriate.
//
//        method ::= 'GET' || 'POST' || 'GET' || 'PUT' || 'DELETE'
//        path: Path to resource collection or instance of resource.
//        options:
//          * id: Id when referencing an instance in a collection.
//          * attr: When delegating to index or update. See create and update.
//          * onSuccess and onError
//
//      Follows the typical RESTful semantics, ie:
//
//        method  path to                     action
//
//        GET     resource collection         index
//        POST    resource collection         create
//        GET     resource instance           read
//        PUT     resource instance           update
//        DELETE  resource instance           delete
//
var Resource = new Class({

  //
  //  initialize:
  //    Args:
  //      * path - path to the resource w/o an instance ID.
  //      * options:
  //        * instName: Resource name to use in the context of
  //          references to a single instance of a resource.
  //        * subResource: A subordinate resource which is used to make up this 
  //          resource. IE: We can express things like:
  //          /importers/<importer ID>/images
  //        * resourceName: Name to use for the resource.
  //
  initialize: function(path, options) {
    this.path = path;
    this.pathPrefix = options.pathPrefix || "";
    this.name = path ? _.last(path.split('/')) : options.resourceName ? options.resourceName : "";
    this.instName = options && _.has(options, 'instName') ? options.instName : this.name;
    this.subResource = options && _.has(options, 'subResource') ? options.subResource : null;

    //
    //  fullPath: Return a full path -
    //    this.path [... + '/' + this.subResource.path]
    //
    this.fullPath = function() {
      var fp = this.path ? this.path : "";
      var sr = this.subResource;
      while (sr) {
        if (sr.path) {
          fp = fp + sr.path;
        }
        sr = sr.subResource;
      }
      return fp;
    };

    //
    //  requestPath: Returns the path to use for a particular request.
    //    Maybe a string or regex. When a resource instance is referenced
    //    the returned value will be a regexp. Hierarchical resources are also
    //    supported which subResources are defined.
    //
    //    Args:
    //      requestType:
    //        create | index | read | update | delete | collection | instance
    //
    //        Note:
    //          collection: References a collection. Equiv. to create | index
    //          instance: References an instance of a resource. Equiv. to reqd | update | delete
    //  
    this.requestPath = function(requestType) {
      if ((requestType !== 'create') && (requestType !== 'index') && (requestType !== 'read') && (requestType !== 'update') && (requestType !== 'delete') && (requestType !== 'collection') && (requestType !== 'instance')) {
        throw Object.create(new Error(),
                            { name: { value: 'InvalidRequestType' },
                              message: { value: 'MediaManager/lib/MediaManagerApiCore.Resource.requestPath: invalid requestType.' } });
      }
      var _requestPath = this.pathPrefix ? this.pathPrefix.replace(/\//g, '\\/') : "";
      var _instIdPath = '\\/(\\$[0-9a-zA-Z\\$\\-_@\\.\\&\\+]+)';
      var _doRequestPath = function(requestType, resource) {
        if ((requestType === 'create') || (requestType === 'index') || (requestType === 'collection')) {
          if (resource.path) {
            _requestPath = _requestPath + resource.path.replace(/\//g, '\\/');
          }
        }
        else {
          if (resource.path) {
            _requestPath = _requestPath + resource.path.replace(/\//g, '\\/') + _instIdPath;
          }
        }
        if (resource.subResource) {
          if (((requestType === 'create') || (requestType === 'index') || (requestType === 'collection')) && resource.path) {
            _requestPath = _requestPath + _instIdPath;
          }
          _doRequestPath(requestType, resource.subResource);
        }
      };
      _doRequestPath(requestType, this);
      if ((requestType === 'create') || (requestType === 'index') || (requestType === 'update') || (requestType === 'collection')) {
        _requestPath = _requestPath + '\\/?';
      }
      return new RegExp('^' + _requestPath + '$');
    };
  },

  index: function(options) { 
    log.info('Resource.index: indexing nothing ...');
    return this; 
  },
  create: function(attr, options) { return this; },
  read: function(id, options) { return this; },
  update: function(id, attr, options) { return this; },
  delete: function(id, options) { return this; },

  //
  //  doRequest: Delegate to one of the index / crud methods.
  //    Args:
  //      method: http verb
  //      options:
  //        id - id of instance in collection.
  //        attr - attributes of instance to create, or update.
  //
  doRequest: function(method, options) {
    log.debug("Resource.doRequest - method:'%s' - opts: '%j'", util.inspect(method), util.inspect(options));
    var id = options.id || undefined;

    if (id) {
      if (method === 'GET') {
        return this.read(id, options);
      }
      else if (method === 'PUT') {
        return this.update(id, options.attr, options);
      }
      else if (method === 'DELETE') {
        return this.delete(id,options);
      }
    }
    else {
      if (method === 'GET') {
        log.info('Resource.doRequest: invoking index ...');
        return this.index(options);
      }
      else if (method === 'POST') {
        return this.create(options.attr, options);
      }
      else if (method === 'DELETE') {
        return this.delete(null, options);
      }
    }
  },

  //
  //  doCallbacks: Generate the response body given the passed in
  //    result, and relevant options. For relevant options, see
  //    createResponseBody.
  //
  doCallbacks: function(status, result, options) {
    log.info('Resource.doCallbacks: w/ status - %s, path - %s', status, this.path);
    if (result) {
      options.rep = result;
    }
    if (status === 200) {
      if (_.has(options, 'onSuccess') && options.onSuccess) {
        var rb = this.createResponseBody(0, options);
        log.trace('Resource.doCallbacks: response body of - ' + JSON.stringify(rb));
        options.onSuccess(rb);
      }
    }
    else if (_.has(options, 'onError') && options.onError) {
      options.onError(this.createResponseBody(1, options));
    }
  },

  //
  //  createResponseBody:
  //    Args:
  //      status: 0 or 1 status.
  //      options:
  //        * resourceName: attribute name to use to return the object 
  //          representation (rep). If not supplied the resource name
  //          is used, which may be 'name' or 'instName', subject
  //          to isInstRef being supplied.
  //        * isInstRef: If true, the request is a reference to an instance
  //          of the resource, as opposed to a collection. If true,
  //          instName will be the attribute used to return a representation.
  //        * rep: representation of the resource to be return.
  //        * errorCode: Error code to identify the error condition.
  //        * errorMessage: Error message.
  //
  createResponseBody: function(status, options) {
    log.info('Resource.createResponseBody: Creating response body w/ status - %s', status);
    var body = {
      status: status
    };
    if (_.has(options, 'errorCode') && _.isNumber(options.errorCode)) {
      body.error_code = options.errorCode;
    }
    if (_.has(options, 'errorMessage') && _.isString(options.errorMessage)) {
      body.error_message = options.errorMessage;
    }
    if (_.has(options, 'rep')) {
      var resName = undefined;

      if (_.has(options, 'resourceName')) {
        resName = options.resourceName;
      }
      else {
        resName = _.has(options, 'isInstRef') && options.isInstRef ? this.instName : this.name;
      }
      if (resName) {
        try {
          var rep = this.transformRep(options.rep, options);

          if (_.has(rep, resName) && _.has(rep, 'paging')) {
            body[resName] = rep[resName];
            body["paging"] = rep["paging"];
          }
          else {
            body[resName] = rep;
          }
          log.debug('createResponseBody: rep transformed!');
        }
        catch (e) {
          log.error('createResponseBody: Error transforming rep, error - ' + e);
          body.status = 1;
          body.error_code = errorCodes.UNKNOWN_ERROR;
          body.error_message = errors.UNKNOWN_ERROR.message;
        }
      }
    }
    return body;
  },

  //
  //  httpResponseStatusCode: Generate an HTTP status code
  //    based upon the status / error_code set in the response
  //    body. The HTTP status should be completely computable
  //    from (status, error_code). See errorCodes at the bottom of the module.
  //
  //    In general if responseBody.errorCode is:
  //      errorCodes.UNKNOWN_ERROR -> 500
  //      any other error codes -> 404
  //      else -> 500
  //
  //    Args:
  //      responseBody
  //
  httpResponseStatusCode: function(responseBody) {
    var status = 500;

    if (_.has(responseBody, 'status')) {
      if (responseBody.status === 0) {
        status = 200;
      }
      else if (_.has(responseBody, 'error_code')) {
        var code = responseBody.error_code;

        var error = _.find(errors, function(err) { return code === err.code; });

        if (error) {
          status = error.httpStatus;
        }
      }
    }
    return status;
  },

  //
  //  transformRep: Override this to transform the representation of the resource.
  //    Args:
  //      rep: Representation to transform.
  //      options: Feel free to pass options.
  //
  transformRep: function(rep, options) {
    log.info('Resource.transformRep: Doing default transformation...');
    return rep;
  }

});

//
// Images Resource:
//
//    Args:
//      postTransformHook: Invoked after the resource is transformed but before it is returned.
//        IE: Could be used for something like re-writing URLs to resources.
//
var Images = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    log.info('Images.initialize: Initialized, path - %s, name - %, instance - %s', this.path, this.name, this.instName);

    this.postTransformHook = options.postTransformHook || function(r) { return r; };

    //
    //  Image Service attrs -> short form attributes.
    //
    this._shortFormAttrs = {
      oid: 'id',
      app_id: 'app_id',
      batch_id: 'importer_id',
      name: 'name',
      url: 'url',
      geometry: 'geometry',
      size: 'size',
      filesize: 'filesize',
      taken_at: 'taken_at',
      created_at: 'created_at',
      in_trash : 'in_trash',
      variants: 'variants',
      tags: 'tags'
    };
    //
    //  Image Service attrs -> full form attributes.
    //
    this._fullFormAttrs = {
      oid: 'id',
      app_id: 'app_id',
      batch_id: 'importer_id',
      name: 'name',
      path: 'path',
      import_root_dir: 'import_root_dir',
      disposition: 'disposition',
      url: 'url',
      format: 'format',
      geometry: 'geometry',
      size: 'size',
      filesize: 'filesize',
      checksum: 'checksum',
      taken_at: 'taken_at',
      created_at: 'created_at',
      variants: 'variants',
      tags: 'tags',
      in_trash : 'in_trash'
    };
    log.debug('Images.initialize: Desired full form attributes - %s', JSON.stringify(_.values(this._fullFormAttrs)));
  },

  index: function(options) {
    log.info('Images.index: Indexing for path - ' + this.path + ', query - ' + util.inspect(options.query));
    options = options || {};
    var that = this;

    var indexOptions = buildIndexOptions(options);
    log.info('Images.index: index options - ' + util.inspect(indexOptions));
    imageService.index(indexOptions,function(err, result) {
      var status = 200;
      if (err) {
        log.error('Images.index', 'error from image service - ' + err);
        status = errors[errors.UNKNOWN_ERROR].httpStatus;
        options.errorCode = errorCodes.UNKNOWN_ERROR;
        options.errorMessage = err;
      }
      log.info('Images.index: invoking callback with status - %s, path - %s', status, that.path);
      that.doCallbacks(status, result, options);
    });
    return that;
  },

  read: function(id, options) { 
    log.info('Images.read: Reading for path - %s, id - %s', this.path, id);
    var that = this;
    imageService.show(removePrefix(id), null, function(err, result) {
      var status = 200;
      if (err) {
        log.info('Images.read: error from image service - %s', err);
        status = errors[errors.UNKNOWN_ERROR].httpStatus;
        options.errorCode = errorCodes.UNKNOWN_ERROR;
        options.errorMessage = err;
      }
      // log.info('Images.read', 'got result of - ' + JSON.stringify(result));
      log.info('Images.read: invoking callback with status - %s, path - %s, id - %s', status, that.path, id);
      var callbackOptions = options ? _.clone(options) : {};
      callbackOptions['isInstRef'] = true;
      that.doCallbacks(status, result, callbackOptions);
    });
    return this; 
  },

  update: function(id, attr, options) {
    log.info('Images.read: Updating for path - %s, id - %s', this.path, id);
    var that = this;
    var status = 200;
    var theImage = null;
    var isTrashOperation = false;
    async.waterfall([


      // check if is a trash operation
      function(next){
        if(options && options.query){
          if(_.isString(options.query.in_trash) ){
            isTrashOperation = true;
            if(options.query.in_trash==="true"){
  
              imageService.sendToTrash([removePrefix(id)],function(err, theSentToTrashImages){
                if (err) {
                  log.error('Images.update.imageService.sendToTrash: error from image service - %s', err);
                  status = errors[errors.UNKNOWN_ERROR].httpStatus;
                  options.errorCode = errorCodes.UNKNOWN_ERROR;
                  options.errorMessage = err;
                }else{
                  theImage = theSentToTrashImages[0];
                }
                next();
              });
            }else
            if(options.query.in_trash==="false"){
              imageService.restoreFromTrash([removePrefix(id)],function(err, theRestoredFromTrashImages){
                if (err) {
                  log.error('Images.update.imageService.restoreFromTrash: error from image service - %s', err);
                  status = errors[errors.UNKNOWN_ERROR].httpStatus;
                  options.errorCode = errorCodes.UNKNOWN_ERROR;
                  options.errorMessage = err;
                }else{
                  theImage = theRestoredFromTrashImages[0];
                }
                next();
              });
            }
            else{
              next();
            }

        }
        } else{
          next();
        }

      },
      //1) retrieve the image
      function(next){
        if(!isTrashOperation){
          imageService.show(removePrefix(id),null,function(err,imgOut){

            if (err) {
              log.error('Images.update.show: error from image service - %s', err);
              status = errors[errors.UNKNOWN_ERROR].httpStatus;
              options.errorCode = errorCodes.UNKNOWN_ERROR;
              options.errorMessage = err;
            }else{
             theImage=imgOut;
            }
            next();
          });
        }else{
          next();
        }
      },
      //2) update the fields
      function(next){
        if(!isTrashOperation){
        //TODO set fields
        }else{
          next();
        }


      },

      //3) call saveOrUpdate
      function(next){
        if(!isTrashOperation){
          imageService.saveOrUpdate(
          {"doc":theImage, "tried":0},  function (err, result) {
            if (err) {
              log.error('Images.update.saveOrUpdate: error from image service - %s', err);
              status = errors[errors.UNKNOWN_ERROR].httpStatus;
              options.errorCode = errorCodes.UNKNOWN_ERROR;
              options.errorMessage = err;
            }
              next();
            });
        }
        else{
          next();
        }
      }

    ], function () {

      log.info('Images.update: invoking callback with status - %s, path - %s, id - %s', status, that.path, id);
      var callbackOptions = options ? _.clone(options) : {};
      callbackOptions['isInstRef'] = true;
      that.doCallbacks(status, theImage, callbackOptions);
      return that;
    });

  },

  delete: function (id, options) {
    log.info('Images.delete: Deleting for path - %s', +this.path);
    options = options || {};
    var that = this;

    if (id) {//delete single image

      async.waterfall([

        function (next) {

          imageService.deleteImages([removePrefix(id)], next);

        }
      ], function (err, result) {

        var status = 200;
        if (err) {
          log.error('Images.delete', 'error from image service - ' + err);
          status = errors[errors.UNKNOWN_ERROR].httpStatus;
          options.errorCode = errorCodes.UNKNOWN_ERROR;
          options.errorMessage = err;
        }

        log.info('Images.delete: invoking callback with status - %s, path - %s', status, that.path);
        that.doCallbacks(status, result, options);
        return that;
      });

    }
    else //delete a collection of images
    if (options && options.query && options.query.trashState) {
      if (options.query.trashState === "in") {

        async.waterfall([
          function (next) {
            imageService.emptyTrash(next);
          }
        ], function (err, result) {

          var status = 200;
          if (err) {
            log.error('Images.delete', 'error from image service - ' + err);
            status = errors[errors.UNKNOWN_ERROR].httpStatus;
            options.errorCode = errorCodes.UNKNOWN_ERROR;
            options.errorMessage = err;
          }

          log.info('Images.delete: invoking callback with status - %s, path - %s', status, that.path);
          that.doCallbacks(status, result, options);
          return that;
        });

      } else {

        var trashStateFilter = {};
        trashStateFilter.trashState = options.query.trashState;

        var oidsToDelete = [];
        async.waterfall([
          function (next) {
            imageService.findImagesByTrashState(trashStateFilter, function (err, results) {
              if (err) {
                next(err);
              }
              else {
                oidsToDelete = _.pluck(results, "oid");
                next(null);
              }

            });
          },
          function (next) {
            imageService.deleteImages(oidsToDelete, next);
          }
        ], function (err, result) {

          var status = 200;
          if (err) {
            log.error('Images.delete', 'error from image service - ' + err);
            status = errors[errors.UNKNOWN_ERROR].httpStatus;
            options.errorCode = errorCodes.UNKNOWN_ERROR;
            options.errorMessage = err;
          }

          log.info('Images.delete: invoking callback with status - %s, path - %s', status, that.path);
          that.doCallbacks(status, result, options);
          return that;
        });

      }

    } else {
      status = errors[errors.BAD_REQUEST].httpStatus;
      options.errorCode = errorCodes.BAD_REQUEST;
      options.errorMessage = 'Image.delete Bad request.';
      that.doCallbacks(status,
        options.attr,
        options);
      return that;
    }

  },

  //
  //  transformRep: ImageService returns data which is transformed according
  //  to API specifications.
  //
  //    Args:
  //      * rep: The result from the image service.
  //      * options:
  //        * isInstRef: If true, we are referencing an instance of an image,
  //          otherwise rep is a collection (array of result).
  //
  transformRep: function(rep, options) {
    log.info('Images.transformRep: Doing transform, path - %s', this.path);
    var that = this;
    if (options && _.has(options, 'isInstRef') && options.isInstRef) {
      log.info('Images.transformRep: transforming instance to full form...');
      try {
        return that.postTransformHook(that._transformToFullFormRep(rep), rep);
      }
      catch (e) {
        log.error('Images.transformRep: Error during transformation - ' + e);
        return {};
      }
    }
    else {
      if (_.isArray(rep)) {
        log.info('Images.transformRep: transforming collection to array of short forms...');
        log.info('Images.transformRep: type of - %s, desired short form attributes - %', 
						typeof(that._shortFormAttrs), JSON.stringify(_.values(that._shortFormAttrs)));
        rep.reverse();
        var newRep = [];
        _.each(rep, 
               function(aRep) {
                 try {
                   var tRep = that.postTransformHook(that._transformToShortFormRep(aRep), aRep);
                   if (tRep) {
                     newRep.push(tRep);
                   }
                 }
                 catch (e) {
                   log.error('Images.transformRep: Error during transformation - ' + e);
                 }
               });
        return newRep;
      }
    }
    return rep;
  },

  _transformToShortFormRep: function(rep) {
    var newRep = {};
    log.trace('Images._transformToShortFormRep', 'will process short form attributes - ' + JSON.stringify(_.keys(this._shortFormAttrs)));
    return this._transformAttrs(newRep, rep, this._shortFormAttrs);
  },

  _transformToFullFormRep: function(rep) {
    var newRep = {};
    return this._transformAttrs(newRep, rep, this._fullFormAttrs);
  },

  _transformAttrs: function(newRep, rep, attrs) {
    try {
      var logMsg = 'Transforming ';
      if (_.has(rep, 'oid')) {
        logMsg = logMsg + 'resource w/ id - ' + rep.oid;
      }
      if (_.has(rep, 'path')) {
        logMsg = logMsg + ', path - ' + rep.path;
      }
      if (_.has(rep, 'name')) {
        logMsg = logMsg + ', name - ' + rep.name;
      }
      log.debug('Images._transformAttrs', logMsg);
      log.trace('Images._transformAttrs', 'processing attributes - ' + JSON.stringify(_.keys(attrs)));
      log.trace('Images._transformAttrs', 'rep has attributes - ' + JSON.stringify(_.keys(rep)));
      var that = this;
      _.each(_.keys(attrs), 
             function(attr) {
               if (attrs[attr] === 'id') {
                 //
                 //  Object IDs begin with '$' followed by the object ID itself.
                 //
                 newRep['id'] = _.has(rep, attr) ? '$' + rep[attr] : undefined;
               }
               else if (attrs[attr] === 'app_id') {
                 //
                 //  Likewise, object IDs begin with '$' followed by the object ID itself.
                 //
                 newRep['app_id'] = _.has(rep, attr) ? '$' + rep[attr] : undefined;
               }
               else if (attrs[attr] === 'importer_id') {
                 //
                 //  Likewise...
                 //
                 newRep['importer_id'] = _.has(rep, attr) ? '$' + rep[attr] : undefined;
               }
               else if (attr === 'variants') {
                 if (_.has(rep, 'variants')) {
                   var variants = [];
                   newRep[attrs.variants] = variants;
                   _.each(rep.variants, 
                          function(variant) {
                            variants.push(that._transformToShortFormRep(variant));
                          });
                 }
               }
               else {
                 newRep[attrs[attr]] = _.has(rep, attr) ? rep[attr] : undefined;
               }
             });
      //
      //  The following should go away eventually as the ImageService 
      //  matures.
      //
      if ((!_.has(newRep, 'id') || (newRep.id === undefined)) && _.has(rep, '_id')) {
        newRep['id'] = '$' + rep._id;
      }
      if ((!_.has(newRep, 'name') || (newRep.name === undefined)) && _.has(rep, 'path')) {
        newRep['name'] = '$' + _.last(rep.path.split('/'));
        log.info('Images._transformAttrs: updated name attribute to - %s', newRep['name']);
      }
    }
    catch (e) {
      log.error('Images._transformAttrs: error - ' + e);
    }
    return newRep;
  }

});

//
//  Tags Resource:
//
var Tags = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    log.info('Tags.initialize: Initialized, path - %s, name - %, instance - %s', this.path, this.name, this.instName);

  },
  index: function(options) {
    log.info('Tags.index: Indexing for path - %s',+ this.path);
    options = options || {};
    var that = this;

    if (_.isEmpty(options.query)){
      imageService.tagsGetAll(function(err, result) {
        var status = 200;
        if (err) {
          log.error('Tags.index', 'error from image service - ' + err);
          status = errors[errors.UNKNOWN_ERROR].httpStatus;
          options.errorCode = errorCodes.UNKNOWN_ERROR;
          options.errorMessage = err;
        }
        log.info('Tags.index: invoking callback with status - %s, path - %s', status, that.path);
        that.doCallbacks(status, result, options);
      });
    }else
    if(options.query.images){
      //get the ids of the images to get their tags

      var imagesIds = options.query.images.split(",") ;

      //retrieve the tags of a set of images
      imageService.tagsGetImagesTags(removePrefix(imagesIds),function(err, result) {
        var status = 200;
        if (err) {
          log.error('Tags.index', 'error from image service - ' + err);
          status = errors[errors.UNKNOWN_ERROR].httpStatus;
          options.errorCode = errorCodes.UNKNOWN_ERROR;
          options.errorMessage = err;
        }
        log.info('Tags.index: invoking callback with status - %s, path - %s', status, that.path);
        that.doCallbacks(status, result, options);
      });

    }


    return that;
  }

});

//
//  Tagger Resource:
//
var Tagger = new Class({

  Extends:Resource,

  initialize:function (path, options) {
    this.parent(path, options);
    log.info('Tagger.initialize: Initialized, path - %s, name - %, instance - %s', this.path, this.name, this.instName);

  },
  index:function (options) {
    log.info('Tagger.index: Indexing for path - %s', +this.path);
    options = options || {};
    var that = this;

    imageService.tagsGetAll(function (err, result) {
      var status = 200;
      if (err) {
        log.error('Tagger.index', 'error from image service - ' + err);
        status = errors[errors.UNKNOWN_ERROR].httpStatus;
        options.errorCode = errorCodes.UNKNOWN_ERROR;
        options.errorMessage = err;
      }
      log.info('Tagger.index: invoking callback with status - %s, path - %s', status, that.path);
      that.doCallbacks(status, result, options);
    });
    return that;
  },
  create:function (attr, options) {
    var that = this;
    var isValid = false;

    var actions = ['add','replace','remove'];
    var containsAnyAction = _.some(actions,  function(action){
      return _.has(attr, action);
    });
    isValid = containsAnyAction;

    if (isValid) {

      if (_.has(attr, 'add')) {
        log.info('Tagger.add: Payload - %s', JSON.stringify(attr));
        var listOfImages = attr.add.images;
        var listOfTags = attr.add.tags;
        imageService.tagsAdd(removePrefix(listOfImages), listOfTags,
          function (err, result) {
            var status = 200;
            if (err) {
              log.error('Tagger.add: Error in imageService.tagsAdd - %j', err);
              status = errors[errors.UNKNOWN_ERROR].httpStatus;
              options.errorCode = errorCodes.UNKNOWN_ERROR;
              options.errorMessage = err;
            }
            else {
              log.info('Tagger.add: Added tags on images- %j', result);
            }
            that.doCallbacks(status, listOfImages, options);
          }
        );
      }
      else
      if(_.has(attr, 'replace')){
        log.info('Tagger.replace: Payload - %s', JSON.stringify(attr));
        var listOfImages = attr.replace.images;
        var oldTags = attr.replace.oldTags;
        var newTags = attr.replace.newTags;
        imageService.tagsReplace(removePrefix(listOfImages), oldTags,newTags,
          function (err, result) {
            var status = 200;
            if (err) {
              log.error('Tagger.replace: Error in imageService.tagsReplace - %j', err);
              status = errors[errors.UNKNOWN_ERROR].httpStatus;
              options.errorCode = errorCodes.UNKNOWN_ERROR;
              options.errorMessage = err;
            }
            else {
              log.info('Tagger.replace: Replaced tags on images- %j', result);
            }
            that.doCallbacks(status, listOfImages, options);
          }
        );
      }

      else
      if(_.has(attr, 'remove')){
        log.info('Tagger.remove: Payload - %s', JSON.stringify(attr));
        var listOfImages = attr.remove.images;
        var tagsToRemove = attr.remove.tags;
        //var newTags = attr.replace.newTags;
        imageService.tagsRemove(removePrefix(listOfImages), tagsToRemove,
          function (err, result) {
            var status = 200;
            if (err) {
              log.error('Tagger.remove: Error in imageService.tagsRemove - %j', err);
              status = errors[errors.UNKNOWN_ERROR].httpStatus;
              options.errorCode = errorCodes.UNKNOWN_ERROR;
              options.errorMessage = err;
            }
            else {
              log.info('Tagger.remove: Removed tags on images - %j', result);
            }
            that.doCallbacks(status, listOfImages, options);
          }
        );
      }


    }
    else {
      status = errors[errors.BAD_REQUEST].httpStatus;
      options.errorCode = errorCodes.BAD_REQUEST;
      options.errorMessage = 'add or replace MUST be specified in the payload.';
      that.doCallbacks(status,
        options.attr,
        options);
    }
    return this;

  }

});


//
//  Importers Resource:
//
var Importers = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    log.info('Importers.initialize: Initialized, path - %s, name - %s, instance - %s', this.path, this.name, this.instName);
    this.postTransformHook = options.postTransformHook || function(r) { return r; };
    this.imagesPostTransformHook = options.imagesPostTransformHook || function(r) { return r; };
  },

  create: function(attr, options) {
    log.info('Importers.create: Payload - %s', JSON.stringify(attr));
    var that = this;
    options = options || {};
    options.isInstRef = true;
    // we need an Images instance to run the 'static' method 'transformRep' further below
    var IMAGES = new Images('', {postTransformHook: this.imagesPostTransformHook}); 
    var importDir = (attr && _.has(attr, 'import_dir')) ? attr.import_dir : undefined;
    if (importDir) {
      try {
        var importOptions = {
           recursionDepth: (_.has(options, 'query') && _.has(options.query, 'dive') && (options.query.dive === 'false')) ? 1 : 0
          ,saveOriginal: false
          ,retrieveSavedImage:true
          ,desiredVariants: [
            { name: 'thumbnail.jpg', format: 'jpg', width: 132, height: 132},
            // { name: 'web.jpg', format: 'jpg', width: 640, height: 400}, 
            { name: 'full-small.jpg', format: 'jpg', width: 1280, height: 800}
            ]
        };
        
        imageService.importBatchFs(
          importDir,
          function(err, importBatch) {
            var status = 200;
            if (err) { 
              log.error('Importers.create: Error importing image(s) - %j', err);
              status = 500;

              if (err.code === imageService.errorCodes.NO_FILES_FOUND) {
                options.errorCode = errorCodes.NO_FILES_FOUND;
                options.errorMessage = util.format(errors.NO_FILES_FOUND.message, importDir);
              }
              else {
                options.errorCode = errorCodes.UNKNOWN_ERROR;
                options.errorMessage = err.message;
              }
            }
            else {
              log.info('Importers.create: Saved images, batch - %j', importBatch);

              importBatch.once(importBatch.event.STARTED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', importBatch.event.STARTED, that.transformRep(anEvent.data, {isInstRef: true}));
              });

              importBatch.once(importBatch.event.IMGS_CREATED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s', event data: '%j'", anEvent.type, anEvent.emitted_at, importBatch.status, anEvent.data);
                notifications.publish('/importers', 
                                      importBatch.event.IMGS_CREATED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": _.map(anEvent.data, function(image) {
                                          return IMAGES.transformRep(image, {isInstRef: true});
                                        })
                                      });
              });

              importBatch.on(importBatch.event.IMG_CREATED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', 
                                      importBatch.event.IMG_CREATED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": IMAGES.transformRep(anEvent.data, {isInstRef: true}) });
              });
              
              importBatch.on(importBatch.event.IMGS_VARIANT_CREATED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', 
                                      importBatch.event.IMGS_VARIANT_CREATED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": _.map(anEvent.data, function(image) {
                                          return IMAGES.transformRep(image, {isInstRef: true});
                                        })
                                      });
              });

              importBatch.on(importBatch.event.IMG_VARIANT_CREATED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', 
                                      importBatch.event.IMG_VARIANT_CREATED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": IMAGES.transformRep(anEvent.data, {isInstRef: true}) });
              });

              importBatch.on(importBatch.event.IMGS_IMPORTED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', 
                                      importBatch.event.IMGS_IMPORTED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": _.map(anEvent.data, function(image) {
                                          return IMAGES.transformRep(image, {isInstRef: true});
                                        })
                                      });
              });

              importBatch.on(importBatch.event.IMG_IMPORTED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', 
                                      importBatch.event.IMG_IMPORTED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": IMAGES.transformRep(anEvent.data, {isInstRef: true}) });
              });

              importBatch.once(importBatch.event.COMPLETED, function(anEvent) {
                log.debug("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', importBatch.event.COMPLETED, that.transformRep(anEvent.data, {isInstRef: true}));
              });

            }
            that.doCallbacks(status, importBatch, options);
          },
          importOptions
        );
      }
      catch (err) {
        log.error('Importers.create: Error saving image (2) - %s', err);
        status = errors[errors.UNKNOWN_ERROR].httpStatus;
        options.errorCode = errorCodes.UNKNOWN_ERROR;
        options.errorMessage = err;
        that.doCallbacks(status,
                         {},
                         options);
      }
    }
    else {
      status = errors[errors.BAD_REQUEST].httpStatus;
      options.errorCode = errorCodes.BAD_REQUEST;
      options.errorMessage = 'import_dir MUST be specified in the payload.';
      that.doCallbacks(status,
                       options.attr,
                       options);
    }
    return this; 
  },

  index: function(options) {
    var lp = 'Importers.index: ';
    log.info(lp + 'Indexing for path - %s', this.path);
    options = options || {};
    var that = this;

    var opts = {
      includeImages: false,
      filterNoImages: true,
      filterAllInTrash: true,
      filterNotStarted: true
    };

    if (_.has(options, 'query')) {
      if (_.has(options.query, 'filter_no_images')) {
        opts.filterNoImages = options.query.filter_no_images;
      }
      if (_.has(options.query, 'filter_all_in_trash')) {
        opts.filterAllInTrash = options.query.filter_all_in_trash;
      }
      if (_.has(options.query, 'filter_not_started')) {
        opts.filterNotStarted = options.query.filter_not_started;
      }
    }

    if (_.has(options, 'query') && _.has(options.query, 'cursor')) {
      if (_.has(options.query, 'page_size')) {
        opts.pageSize = parseInt(options.query.page_size);
      }
      if (_.has(options.query, 'page_to')) {
        opts.pageTo = options.query.page_to;
      }
      log.debug(lp + 'Index w/ pagination requested, cursor ' + options.query.cursor + ', query - ' + util.inspect(options.query));
      var cursor = undefined;
      try {
        cursor = JSON.parse(options.query.cursor);
      }
      catch (e) {
        log.debug(lp + 'Non-JSON, e - ' + e + ', cursor - ' + options.query.cursor);
        cursor = -1;
      }
      imageService.Importers.pagedIndex(cursor,
                                        opts,
                                        function(err, result) {
                                          var status = 200;
                                          if (err) {
                                            log.error('Importers.index: error from image service - %s', err);
                                            status = errors[errors.UNKNOWN_ERROR].httpStatus;
                                            options.errorCode = errorCodes.UNKNOWN_ERROR;
                                            options.errorMessage = err;
                                          }
                                          log.info('Importers.index: invoking callback with status - %s, path- %s', status, that.path);
                                          that.doCallbacks(status, result, options);
                                        });
    }
    else {
      var numBatches = (_.has(options, 'query') && _.has(options.query, 'n')) ? options.query.n : undefined;    
      imageService.Importers.index(numBatches,
                                   opts,
                                   function(err, result) {
                                     var status = 200;
                                     if (err) {
                                       log.error('Importers.index: error from image service - %s', err);
                                       status = errors[errors.UNKNOWN_ERROR].httpStatus;
                                       options.errorCode = errorCodes.UNKNOWN_ERROR;
                                       options.errorMessage = err;
                                     }
                                     log.info('Importers.index: invoking callback with status - %s, path- %s', status, that.path);
                                     that.doCallbacks(status, result, options);
                                   });
    }
    return that;
  },

  read: function(id, options) {
    log.info('Importers.read, Reading for path - %s, id - %s', this.path, id);
    var that = this;
    imageService.importBatchShow(removePrefix(id),
                                 { includeImages: false },
                                 function(err, result) {
                                   var status = 200;
                                   if (err) {
                                     log.error('Importers.show: error from image service - %s', err);
                                     status = errors[errors.UNKNOWN_ERROR].httpStatus;
                                     options.errorCode = errorCodes.UNKNOWN_ERROR;
                                     options.errorMessage = err;
                                   }
                                   log.info('Importers.show: invoking callback with status - %s, path - %s', status, that.path);
                                   var callbackOptions = options ? _.clone(options) : {};
                                   callbackOptions['isInstRef'] = true;
                                   that.doCallbacks(status, result, callbackOptions);
                                 });
    return this;
  },

  update: function(id, attr, options) {
    var that = this;

    log.info('Importers.update: updating for path - %s, id - %s, w/ attr - %j', this.path, id, attr);

    var isRep = this.reverseTransformRep(id, attr);

    log.debug('Importers.update: image service rep - ' + JSON.stringify(isRep));

    imageService.importBatchUpdate(isRep,
                                   {},
                                   function(err, result) {
                                     var status = 200;
                                     var callbackOptions = options ? _.clone(options) : {};
                                     if (err) {
                                       log.error('Importers.update: error from image service - %s', util.inspect(err));
                                       var error = getErrorFromImageService(err);
                                       status = error.httpStatus;                                       
                                       callbackOptions.errorCode = error.code;
                                       callbackOptions.errorMessage = error.message;
                                     }
                                     callbackOptions['isInstRef'] = true;
                                     that.doCallbacks(status, result, callbackOptions);
                                   });

    log.debug('Importers.update: image service invoked...');

    return this;
  },

  //
  //  reverseTransformRep: back to image service representation.
  //
  reverseTransformRep: function(id, rep) {

    var attrMap = {
      "id": "oid",
      "app_id": "app_id",
      "import_dir": "path",
      "state": "status",
      "created_at": "created_at",
      "started_at": "started_at",
      "completed_at": "completed_at",
      "num_to_import": "num_to_import",
      "num_imported": "num_imported",
      "num_success": "num_success",
      "num_error": "num_error"
    };

    var isRep = {};

    _.each(rep, function(value, attr) {
      if (_.has(attrMap, attr)) {
        isRep[attrMap[attr]] = value;
      }
    });

    if (_.has(isRep, 'app_id')) {
      isRep.app_id = isRep.app_id.replace('$', '');
    }

    isRep.oid = id ? id.replace('$', '') : attr.id.replace('$', '');

    if (_.has(isRep, 'status')) {
      isRep.status = isRep.status.toUpperCase();
    }

    return isRep;
  },

  //
  //  transformRep: ImageService returns data which is transformed according
  //  to API specifications.
  //
  //    Args:
  //      * rep: The result from the image service.
  //      * options:
  //        * isInstRef: If true, we are referencing an instance.
  //
  transformRep: function(rep, options) {
    var lp = 'Importers.transformRep: ';
    log.info(lp + 'Doing transform, path - %s', this.path);
    var that = this;
    if (options && _.has(options, 'isInstRef') && options.isInstRef) {
      log.debug('Importers.transformRep: transforming instance to full form, rep - ' + util.inspect(rep));
      var newRep = this._transformOne(rep);
      log.debug('Importers.transformRep: transformed rep - ' + util.inspect(newRep));
      return newRep;
    }
    else {
      if (_.isArray(rep)) {
        log.info('Importers.transformRep: transforming collection...');
        var newRep = [];
        _.each(rep, 
               function(aRep) {
                 var tRep = that._transformOne(aRep);
                 if (tRep) {
                   newRep.push(tRep);
                 }
               });
        return newRep;
      }
      else if (_.has(rep, 'items') && _.has(rep, 'cursors')) {
        //
        // Paged response, rep will look like:
        //
        //  { 
        //    items: [
        //      { cursor: [Object], doc: [Object] },
        //      { cursor: [Object], doc: [Object] },
        //      .
        //      .
        //      { cursor: [Object], doc: [Object] }
        //    ],
        //    cursors: {
        //      start: { key: [Object], id: 'ef3782e5-9c30-467a-a937-683afb833601' },
        //      end: { key: [Object], id: 'ef5430cc-a593-463d-81de-c013852956ae' },
        //      previous: undefined,
        //      next: { key: [Object], id: '76f7cbfa-474f-421c-8159-7b0fe13d3a8e' }
        //    }
        //  }
        //
        var newRep = {};

        log.debug(lp + 'Rep. w/ pagination, items - ' + util.inspect(rep.items) + ', cursors - ' + util.inspect(rep.cursors));

        newRep.importers = [];
        _.each(rep.items, 
               function(item) {
                 var tRep = that._transformOne(item.doc);
                 if (tRep) {
                   newRep.importers.push(tRep);
                 }
               });
        var cursors = {};
        var paging = {
          cursors: cursors,
          page_size: newRep.importers.length
        };
        newRep.paging = paging;
        if (rep.items && rep.items.length) {
          cursors.start = qs.escape(JSON.stringify(rep.items[0].cursor));
          cursors.end = qs.escape(JSON.stringify(rep.items[rep.items.length-1].cursor));
          if (_.has(rep.cursors, 'next')) {
            if (_.isObject(rep.cursors.next)) {
              cursors.next = qs.escape(JSON.stringify(rep.cursors.next));
            }
            else {
              cursors.next = -1;
            }
          }
          if (_.has(rep.cursors, 'previous')) {
            if (_.isObject(rep.cursors.previous)) {
              cursors.previous = qs.escape(JSON.stringify(rep.cursors.previous));
            }
            else {
              cursors.previous = -1;
            }
          }
        }
        return newRep;
      }
    }
    return rep;
  },

  _transformOne: function(rep) {
    var newRep = {};
    try {
      newRep.id = _.has(rep, 'oid') ? '$' + rep.oid : undefined;
      newRep.app_id = _.has(rep, 'app_id') ? '$' + rep.app_id : undefined;
      newRep.import_dir = _.has(rep, 'path') ? rep.path : '';
      newRep.state = rep.status.toLowerCase();
      newRep.created_at = rep.created_at;
      newRep.started_at = rep.started_at;
      newRep.completed_at  = rep.completed_at;
      newRep.num_to_import = rep.getNumToImport();
      newRep.num_imported  = rep.getNumAttempted();
      newRep.num_success   = rep.getNumSuccess();
      newRep.num_error     = rep.getNumError();
    }
    catch (e) {
      log.error('Importers._transformOne: error transforming rep, error - ' + e + ', rep - ' + util.inspect(rep));
      throw e;
    }
    return newRep;
  }

});

//
//  ImportersImages Resource:
//
var ImportersImages = new Class({

  Extends: Resource,

  //
  //  Initialize:
  //    options:
  //      pathPrefix: ie - /v0
  //      subResource: Importers where Importers has a sub-resource of Images.
  //      postTransformHook: Invoked after the resource is transformed but before it is returned.
  //        IE: Could be used for something like re-writing URLs to resources.
  //
  initialize: function(path, options) {
    this.parent(path, options);
    log.info('ImportersImages.initialize: Initialized, path - %s, name - %s, instance - %s', this.path, this.name, this.instName);
    if (!_.has(options, 'subResource')) {
      throw Object.create(new Error(),
                          { name: { value: 'MissingSubResource' },
                            message: { value: 'MediaManager/lib/MediaManagerApiCore.ImportersImages.initialize: Importers subResource required.' } });
    }
    this.importersResource = options.subResource;
    if (!_.has(this.importersResource, 'subResource')) {
      throw Object.create(new Error(),
                          { name: { value: 'MissingSubResource' },
                            message: { value: 'MediaManager/lib/MediaManagerApiCore.ImportersImages.initialize: Importers Images subResource required.' } });
    }
    this.imagesResource = this.importersResource.subResource;

    this.postTransformHook = options.postTransformHook || function(r) { return r; };
  },

  //
  //  index:
  //    Args:
  //      options:
  //        req: The request, S.T. req.params[0] contains the importers instance ID.
  //
  index: function(options) {
    if (options && _.has(options, 'req') && _.has(options.req, 'params')) {
      log.info('ImportersImages.index: Indexing w/ params - %s', options.req.params);
    }
    else {
      log.info('ImportersImages.index: Indexing...');
    }
    options = options || {};
    var that = this;
    var batchId = options.req.params[0];
    var imagesFilter = { includeImages: true };
    if(options.query && options.query.trashState){
      if(options.query.trashState === 'in'){
        imagesFilter.imagesTrashState='in';
      }else
      if(options.query.trashState === 'out'){
        imagesFilter.imagesTrashState='out'
      }else
      if(options.query.trashState === 'any'){
        imagesFilter.imagesTrashState='any'
      }

    }

    imageService.importBatchShow(removePrefix(batchId),
                                  imagesFilter,
                                 function(err, result) {
                                   var status = 200;
                                   if (err) {
                                     log.error('ImportersImages.index: error from image service - %s', err);
                                     status = errors[errors.UNKNOWN_ERROR].httpStatus;
                                     options.errorCode = errorCodes.UNKNOWN_ERROR;
                                     options.errorMessage = err;
                                   }
                                   log.info('ImportersImages.index: invoking callback with status - %s, path - %s', status, that.path);
                                   var callbackOptions = options ? _.clone(options) : {};
                                   callbackOptions['isInstRef'] = true;
                                   callbackOptions['resourceName'] = 'importer';
                                   that.doCallbacks(status, result, callbackOptions);
                                 });
    return this;
  },

  //
  //  transformRep: ImageService returns data which is transformed according
  //  to API specifications for /Importers/<import ID>/images
  //
  //    Args:
  //      * rep: The result from the image service.
  //
  transformRep: function(rep, options) {
    log.info('ImportersImages.transformRep: Doing transform...');
    var newRep = this.importersResource.transformRep(rep, { isInstRef: true,
                                                            resourceName: 'importer' });
    if (newRep) {
      if (rep.images) {
        newRep.images = this.imagesResource.transformRep(rep.images,
                                                         { isInstRef: false,
                                                           resourceName: 'images' } );
      }
      else {
        newRep.images = [];
      }
    }
    this.postTransformHook(newRep, rep);
    return newRep;
  }

});

//
//  StorageSynchronizers Resource:
//
var StorageSynchronizers = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    log.info('StorageSynchronizers.initialize: Initialized, path - %s, name - %s, instance %s', this.path, this.name, this.instName);
  },

  create: function(attr, options) {
    log.info('StorageSynchronizers.create: Payload - %j', attr);
    var that = this;
    options.isInstRef = true;
    var synchronizer = undefined;
    try {
      synchronizer = storage.sync();
      function publishSyncEvent(event, synchronizer) {
        notifications.publish('/storage/synchronizers',
                              event,
                              that.transformRep(synchronizer));
      }
      synchronizer.on('sync.started', 
                      function(synchronizer) {
                        publishSyncEvent('sync.started', synchronizer);
                      });
      synchronizer.on('sync.completed',
                      function(synchronizer) {
                        publishSyncEvent('sync.completed', synchronizer);
                      });
      synchronizer.run();
      that.doCallbacks(200, synchronizer, options);
    }
    catch (err) {
      log.error('StorageSynchronizers.create: Caught error - %s', err);
      var status = errors[errors.UNKNOWN_ERROR].httpStatus;
      options.errorCode = errorCodes.UNKNOWN_ERROR;
      options.errorMessage = err.message;
      try {
        that.doCallbacks(status, synchronizer, options);
      }
      catch (err) {
        that.doCallbacks(status, undefined, options);
      }
    }
  },

  read: function(id, options) {
    log.info('StorageSynchronizers.read: Reading for path - %s, id - %s', this.path, id);
    var that = this;
    options.isInstRef = true;
    var synchronizerId = removePrefix(id);
    var synchronizer = storage.syncState(synchronizerId, 
                                         function(err, synchronizer) {
                                           that.doCallbacks(200, synchronizer, options);
                                         });
  },

  //
  //  transformRep: transform a synchronizer.
  //
  //    Args:
  //      * rep: A synchronizer.
  //      * options:
  //
  transformRep: function(rep, options) {
    log.info('StorageSynchronizers.transformRep: Doing transform, path - %s, id - %s, state - %s', this.path, rep.id, rep.state);
    var newRep = {};
    if (rep) {
      newRep = { id: '$' + rep.id,
                 state: rep.state,
                 push: { id: '$' + rep.push.id,
                         state: rep.push.state },
                 pull: { id: '$' + rep.pull.id,
                         state: rep.pull.state } 
               };
    }
    return newRep;
  }

});

//
//  StorageChangesFeed Resource: Access the changes feed.
//
//    Note, this is NOT currently exposed as a public API. But, the intent is
//    to use this resource internally, in order to monitor the storage engine's
//    changes feed. Once, the changes feed resource is created, monitoring
//    of the feed commences. Document changes are subsequently published 
//    to the notifications API.
//
//    ONLY the create method is implemented. The create method instantiates a 
//    storage.changesFeed instance, and listens for change events which are
//    inturn passed to the notifications API.
//
//    The following events are published to the notifications API:
//
//    doc.<doc type>.<change type>
//
//      <doc type> ::= image | importer
//      <change type> ::= created | updated | deleted
//
var StorageChangesFeed = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    log.info('StorageChangesFeed.initialize: Initialized, path - %s, name - %s, instance %s', this.path, this.name, this.instName);
  },

  //
  //  create: Create a StorageChangesFeed resource. Essentially, triggers monitoring the MediaManagerStorage changesFeed.
  //
  //    Query parameters:
  //
  //      since: DB update sequence to begin monitoring changes from. Only changes where the corresponding update sequence
  //        is greater than this one will be considered.
  //      exclude_app_id: Ignore document changes where the app_id of the doc. 
  //        equals exclude_app_id. For example, it may be desireable to only
  //        get change notifications for documents created/updated/deleted
  //        by another instance of the APP, which have been stored in the
  //        local store via a synchronization of the DBs.
  //
  create: function(attr, options) {
    log.info('StorageChangesFeed.create: Payload - %j, options - %s', attr, options);
    var that = this;

    var since = (options && _.has(options, 'query') && _.has(options.query, 'since')) ? options.query.since : undefined;
    var excludeAppId = (options && _.has(options, 'query') && _.has(options.query, 'exclude_app_id')) ? options.query.exclude_app_id : undefined;
    var changesFeed = undefined;

    try {
      changesFeed = storage.changesFeed({since: since,
                                         excludeAppId: excludeAppId});

      function publishImagesChangeEvent(event) {

        log.info('StorageChangesFeed.create: About to publish image change event, image doc - %j', event.doc);

        var IMAGES = new Images('',{});

        var eventType = event.type;
        var id = event.doc.oid;

        if ((event.type === 'doc.image.deleted') && (event.doc.orig_id)) {
          eventType = 'doc.image.updated';
          id = event.doc.orig_id;
        }
        else if (event.doc.orig_id) {
          id = event.doc.orig_id;
        }
        if (eventType === 'doc.image.deleted') {
          notifications.publish('/storage/changes-feed',
                                eventType,
                                {
                                  "doc_resource": "/images",
                                  "doc": {
                                    "id": '$' + id
                                  }
                                });
        }
        else if (event.doc.orig_id) {
          imageService.show(event.doc.orig_id, null, function(err, result) {
            if (err) {
              log.info('StorageChangesFeed.create: Failed to fetch origin image in order to publish change event, error - %s', err);
            }
            else {
              notifications.publish('/storage/changes-feed',
                                    event.type,
                                    {
                                      "doc_resource": "/images",
                                      "doc": IMAGES.transformRep(result, {isInstRef: true})
                                    });
            }});
        }
        else {
          notifications.publish('/storage/changes-feed',
                                event.type,
                                {
                                  "doc_resource": "/images",
                                  "doc": IMAGES.transformRep(event.doc, {isInstRef: true})
                                });
        }
      }

      function publishImporterChangeEvent(event) {
        log.info('StorageChangesFeed.create: About to publish an importer change event, importer doc - %j', event.doc);

        var IMPORTERS = new Importers('', {});

        if (event.type === 'doc.importer.deleted') {
          notifications.publish('/storage/changes-feed',
                                event.type,
                                {
                                  "doc_resource": "/importers",
                                  "doc": {
                                    "id": '$' + event.doc.oid
                                  }
                                });
        }
        else {
          notifications.publish('/storage/changes-feed',
                                event.type,
                                {
                                  "doc_resource": "/importers",
                                  "doc": IMPORTERS.transformRep(event.doc, {isInstRef: true})
                                });
        }
      }

      changesFeed.on('doc.image.created', 
                      function(event) {
                        publishImagesChangeEvent(event);
                      });
      changesFeed.on('doc.image.updated',
                      function(event) {
                        publishImagesChangeEvent(event);
                      });
      changesFeed.on('doc.image.deleted',
                      function(event) {
                        publishImagesChangeEvent(event);
                      });
      changesFeed.on('doc.importer.created', 
                      function(event) {
                        publishImporterChangeEvent(event);
                      });
      changesFeed.on('doc.importer.updated',
                      function(event) {
                        publishImporterChangeEvent(event);
                      });
      changesFeed.on('doc.importer.deleted',
                      function(event) {
                        publishImporterChangeEvent(event);
                      });
      changesFeed.listen();
      that.doCallbacks(200, changesFeed, options);
    }
    catch (err) {
      log.error('StorageChangesFeed.create: Caught error - %s', err);
      var status = errors[errors.UNKNOWN_ERROR].httpStatus;
      options.errorCode = errorCodes.UNKNOWN_ERROR;
      options.errorMessage = err.message;
      try {
        that.doCallbacks(status, changesFeed, options);
      }
      catch (err) {
        that.doCallbacks(status, undefined, options);
      }      
    }
  }

});


function buildIndexOptions(options){
  var indexOptions = {};

  if(options.query && _.has(options.query, 'tags')){

    if (_.size(options.query.tags) === 0) {
      //
      // Return untagged. Filter anything with tags.
      //
      indexOptions.filter = {
        field: 'tags',
        op: 'ne',
        data: []
      };
    }
    else {
      var filter = {};
      indexOptions.filter=filter;

      filter.rules = [];

      filter.groupOp = _.has(options.query, 'tag_query_op') ? options.query.tag_query_op : 'OR';

      var tagsArray =   options.query.tags.split(",");
      _.each(tagsArray,
             function(tag) {
               var rule = {
                 "field":"tags",
                 "op":"eq",
                 "data":tag
               };
               filter.rules.push(rule);
             });


      if(_.size(filter.rules) === 0){
        indexOptions = null;
      }
    }
  }

  if(options.query && options.query.trashState){
    indexOptions.trashState = options.query.trashState;
  }

  if (_.isEmpty(indexOptions)){
    indexOptions = null;
  }

  return indexOptions;
}

/**
 * removes the $ in a string oid or array of string oids
 *
 */
function removePrefix(oids){
  if (_.isString(oids)){
    return oids.replace('$', '');
  }

  if (_.isArray(oids)){
    var cleanOids = [];
    for (var i = 0; i < oids.length; i++) {
      cleanOids.push(oids[i].replace('$', ''));
    }
    return cleanOids;
  }

  log.error("------------- removePrefix: Not valid oids parameter -----------------");

}

//
// Errors / error codes:
//
var errorCodes = {
  UNKNOWN_ERROR: -1,
  NO_FILES_FOUND: 1,
  CONFLICT: 2,
  ATTRIBUTE_VALIDATION_FAILURE: 3,
  NOT_IMPLEMENTED: 4,
  BAD_REQUEST: 5
};
exports.errorCodes = errorCodes;

var errors = {
  UNKNOWN_ERROR: {
    code: errorCodes.UNKNOWN_ERROR,
    httpStatus: 500,
    message: "Unknown error occurred."
  },

  NO_FILES_FOUND: {
    code: errorCodes.IMPORT_NO_FILES_FOUND,
    httpStatus: 404,
    message: "No files found in directory %s ."
  },

  CONFLICT: {
    code: errorCodes.CONFLICT,
    httpStatus: 409,
    message: "Entity conflict, a revision of the entity has been generated with attribute values which would conflict with new values being set."
  },

  ATTRIBUTE_VALIDATION_FAILURE: {
    code: errorCodes.ATTRIBUTE_VALIDATION_FAILURE,
    httpStatus: 404,
    message: "Attributes being set have failed validation."
  },

  NOT_IMPLEMENTED: {
    code: errorCodes.NOT_IMPLEMENTED,
    httpStatus: 404,
    message: "Request not yet supported."
  },

  BAD_REQUEST: {
    code: errorCodes.BAD_REQUEST,
    httpStatus: 400,
    message: "Invalid request, please consult the API documentation."
  }

};

function getErrorFromImageService(err) {
  var error = errors.UNKNOWN_ERROR;
  if (err.code === imageService.errorCodes.NO_FILES_FOUND) {
    error = errors.NO_FILES_FOUND;
  }
  else if (err.code === imageService.errorCodes.CONFLICT) {
    error = errors.CONFLICT;
  }
  else if (err.code === imageService.errorCodes.ATTRIBUTE_VALIDATION_FAILURE) {
    error = errors.ATTRIBUTE_VALIDATION_FAILURE;
  }
  else if (err.code === imageService.errorCodes.NOT_IMPLEMENTED) {
    error = errors.NOT_IMPLEMENTED;
  }
  return error;
}

exports.errors = errors;
