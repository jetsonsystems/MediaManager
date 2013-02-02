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
var imageService = require('ImageService');
var imageServicePackage = require('ImageService/package.json');

//
// Require the storage module. Don't instantiate the module by invoking
// it until we have a proper configuration.
//
var storageModule = require('MediaManagerStorage');
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
  options = options || {}
  if (!_.has(options, 'singleton')) {
    options.singleton = true;
  }
  if (options.singleton && (mediaManagerApi !== null)) {
    //
    //  Not the first time, you can leave off the config. Otherwise, it must be the same as the previous one.
    //
    if (config && !_.isEqual(mediaManagerApi.config, config)) {
      var msg = 'MediaManagerApi/lib/MediaManagerApiCore.js: Reinstantiating with a different configuration. Module is a singleton.';
      throw Object.create(new Error(msg),
                          { name: { value: 'ReinstantiatingWithDifferentConfig' },
                            message: { value: msg } });
    }
    cLogger.log('mediaManagerApiModule', 'Returning alreading created module instance!');
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
    Importers: { value: Importers },
    ImportersImages: { value: ImportersImages },
    StorageSynchronizers: { value: StorageSynchronizers },
    StorageChangesFeed: { value: StorageChangesFeed }
  });

  cLogger.log('mediaManagerApiModule', 'Returning new module instance...');

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

  cLogger.log('init', 'Initializing...');

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
  storage = storageModule(useConfig.db, options);
};

var ConsoleLogger = function(debugLevel) {

  this.debugLevel = debugLevel;
  this.module = '/MediaManager/MediaManagerApi/lib/MediaManagerApiCore';

  this.log = function(context, msg) {
    this.debugLevel <= 0 || console.log(this.module + '.' + context + ': ' + msg);
    return;
  }
};

var cLogger = new ConsoleLogger(1);

cLogger.log('', 'Using ImageService version - ' + imageServicePackage.version);

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
      }
      _doRequestPath(requestType, this);
      if ((requestType === 'create') || (requestType === 'index') || (requestType === 'collection')) {
        _requestPath = _requestPath + '\\/?';
      }
      return new RegExp('^' + _requestPath + '$');
    };
  },

  index: function(options) { 
    cLogger.log('Resource.index', 'indexing nothing ...');
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
    cLogger.log('Resource.doRequest', 'method - ' + method);
    var id = options.id || undefined;

    if (id) {
      if (method === 'GET') {
        return this.read(id, options);
      }
      else if (method === 'PUT') {
        return this.update(id, options.attr, options);
      }
      else if (method === 'DELETE') {
        return this.update(id, options);
      }
    }
    else {
      if (method === 'GET') {
        cLogger.log('Resource.doRequest', 'invoking index ...');
        return this.index(options);
      }
      else if (method === 'POST') {
        return this.create(options.attr, options);
      }
    }
  },

  //
  //  doCallbacks: Generate the response body given the passed in
  //    result, and relevant options. For relevant options, see
  //    createResponseBody.
  //
  doCallbacks: function(status, result, options) {
    cLogger.log('Resource.doCallbacks', 'w/ status - ' + status + ', path - ' + this.path);
    if (result) {
      options.rep = result;
    }
    if (status === 200) {
      if (_.has(options, 'onSuccess') && options.onSuccess) {
        options.onSuccess(this.createResponseBody(0, options));
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
    cLogger.log('Resource.createResponseBody', 'Creating response body w/ status - ' + status);
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
        body[resName] = this.transformRep(options.rep, options);
      }
    }
    return body;
  },

  //
  //  transformRep: Override this to transform the representation of the resource.
  //    Args:
  //      rep: Representation to transform.
  //      options: Feel free to pass options.
  //
  transformRep: function(rep, options) {
    cLogger.log('Resource.transformRep', 'Doing default transformation...');
    return rep;
  }

});

//
//  Images Resource:
//
var Images = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    cLogger.log('Images.initialize', 'Initialized, path - ' + this.path + ', name - ' + this.name + ', instance name - ' + this.instName);
    //
    //  Image Service attrs -> short form attributes.
    //
    this._shortFormAttrs = {
      oid: 'id',
      name: 'name',
      url: 'url',
      geometry: 'geometry',
      size: 'size',
      filesize: 'filesize',
      taken_at: 'taken_at',
      created_at: 'created_at',
      variants: 'variants'
    };
    //
    //  Image Service attrs -> full form attributes.
    //
    this._fullFormAttrs = {
      oid: 'id',
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
      variants: 'variants'
    };
    cLogger.log('Images.initialize', 'Desired full form attributes - ' + JSON.stringify(_.values(this._fullFormAttrs)));
    cLogger.log('Images.initialize', 'Desired short form attributes - ' + JSON.stringify(_.values(this._shortFormAttrs)));
  },

  index: function(options) {
    cLogger.log('Images.index', 'Indexing for path - ' + this.path);
    options = options || {};
    var that = this;
    imageService.index(function(err, result) {
      var status = 200;
      if (err) {
        cLogger.log('Images.index', 'error from image service - ' + err);
        status = 500;
        options.errorCode = -1;
        options.errorMessage = err;
      }
      cLogger.log('Images.index', 'invoking callback with status - ' + status + ', path - ' + that.path);
      that.doCallbacks(status, result, options);
    });
    return that;
  },

  read: function(id, options) { 
    cLogger.log('Images.read', 'Reading for path - ' + this.path + ', id - ' + id);
    var that = this;
    imageService.show(id.replace('$', ''), function(err, result) {
      var status = 200;
      if (err) {
        cLogger.log('Images.read', 'error from image service - ' + err);
        status = 500;
        options.errorCode = -1;
        options.errorMessage = err;
      }
      // cLogger.log('Images.read', 'got result of - ' + JSON.stringify(result));
      cLogger.log('Images.read', 'invoking callback with status - ' + status + ', path - ' + that.path + ', id - ' + id);
      var callbackOptions = options ? _.clone(options) : {};
      callbackOptions['isInstRef'] = true;
      that.doCallbacks(status, result, callbackOptions);
    });
    return this; 
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
    cLogger.log('Images.transformRep', 'Doing transform, path - ' + this.path);
    var that = this;
    if (options && _.has(options, 'isInstRef') && options.isInstRef) {
      cLogger.log('Images.transformRep', 'transforming instance to full form...');
      return that._transformToFullFormRep(rep);
    }
    else {
      if (_.isArray(rep)) {
        cLogger.log('Images.transformRep', 'transforming collection to array of short forms...');
        cLogger.log('Images.transformRep', 'type of - ' + typeof(that._shortFormAttrs) + ', desired short form attributes - ' + JSON.stringify(_.values(that._shortFormAttrs)));
        rep.reverse();
        var newRep = [];
        _.each(rep, 
               function(aRep) {
                 var tRep = that._transformToShortFormRep(aRep);
                 if (tRep) {
                   newRep.push(tRep);
                 }
               });
        return newRep;
      }
    }
    return rep;
  },

  _transformToShortFormRep: function(rep) {
    var newRep = {};
    cLogger.log('Images._transformToShortFormRep', 'will process short form attributes - ' + JSON.stringify(_.keys(this._shortFormAttrs)));
    return this._transformAttrs(newRep, rep, this._shortFormAttrs);
  },

  _transformToFullFormRep: function(rep) {
    var newRep = {};
    return this._transformAttrs(newRep, rep, this._fullFormAttrs);
  },

  _transformAttrs: function(newRep, rep, attrs) {
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
    cLogger.log('Images._transformAttrs', logMsg);
    cLogger.log('Images._transformAttrs', 'processing attributes - ' + JSON.stringify(_.keys(attrs)));
    cLogger.log('Images._transformAttrs', 'rep has attributes - ' + JSON.stringify(_.keys(rep)));
    var that = this;
    _.each(_.keys(attrs), 
           function(attr) {
             if (attrs[attr] === 'id') {
               //
               //  Object IDs begin with '$' followed by the object ID itself.
               //
               newRep['id'] = _.has(rep, attr) ? '$' + rep[attr] : undefined;
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
      cLogger.log('Images._transformAttrs', 'updated name attribute to - ' + newRep['name']);
    }
    return newRep;
  }

});

//
//  Importers Resource:
//
var Importers = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    cLogger.log('Importers.initialize', 'Initialized, path - ' + this.path + ', name - ' + this.name + ', instance name - ' + this.instName);
  },

  create: function(attr, options) {
    cLogger.log('Importers.create', 'Payload - ' + JSON.stringify(attr));
    var that = this;
    options.isInstRef = true;
    // we need an Images instance to run the 'static' method 'transformRep' further below
    var IMAGES = new Images('',{}); 
    var importDir = (attr && _.has(attr, 'import_dir')) ? attr.import_dir : undefined;
    if (importDir) {
      try {
        var importOptions = {
          recursionDepth: (_.has(options, 'query') && _.has(options.query, 'dive') && (options.query.dive === 'false')) ? 1 : 0,
          saveOriginal: false,
          desiredVariants: [{ name: 'thumbnail.jpg', format: 'jpg', width: 132, height: 132}, 
                            { name: 'web.jpg', format: 'jpg', width: 640, height: 400}, 
                            { name: 'full-small.jpg', format: 'jpg', width: 1280, height: 800}]
        };
        
        imageService.importBatchFs(
          importDir,
          function(err, importBatch) {
            var status = 200;
            if (err) { 
              cLogger.log('Importers.create', 'Error saving image (1) - ' + JSON.stringify(err));
              status = 500;
              options.errorCode = -1;
              options.errorMessage = err;
            }
            else {
              cLogger.log('Importers.create', 'Saved images, batch - ' + JSON.stringify(importBatch));

              importBatch.once(importBatch.event.STARTED, function(anEvent) {
                console.log("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', importBatch.event.STARTED, that.transformRep(anEvent.data, {isInstRef: true}));
              });
              
              importBatch.on(importBatch.event.IMG_SAVED, function(anEvent) {
                console.log("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', 
                                      importBatch.event.IMG_SAVED, 
                                      { "id": "$" + importBatch.oid,
                                        "doc_resource": "/images",
                                        "doc": IMAGES.transformRep(anEvent.data, {isInstRef: true}) });
              });

              importBatch.once(importBatch.event.COMPLETED, function(anEvent) {
                console.log("event '%s' emitted at '%s', importBatch status is: '%s'", anEvent.type, anEvent.emitted_at, importBatch.status);
                notifications.publish('/importers', importBatch.event.COMPLETED, that.transformRep(anEvent.data, {isInstRef: true}));
              });
            }
            that.doCallbacks(status, importBatch, options);
          },
          importOptions
        );
      }
      catch (err) {
        cLogger.log('Importers.create', 'Error saving image (2) - ' + err);
        status = 500;
        options.errorCode = -1;
        options.errorMessage = err;
        that.doCallbacks(status,
                         {},
                         options);
      }
    }
    else {
      status = 500;
      options.errorCode = -1;
      options.errorMessage = 'import_dir MUST be specified in the payload.';
      that.doCallbacks(status,
                       options.attr,
                       options);
    }
    return this; 
  },

  index: function(options) {
    cLogger.log('Importers.index', 'Indexing for path - ' + this.path);
    options = options || {};
    var that = this;
    var numBatches = (_.has(options, 'query') && _.has(options.query, 'n')) ? options.query.n : 1;
    
    imageService.importBatchFindRecent(numBatches,
                                       {},
                                       function(err, result) {
                                         var status = 200;
                                         if (err) {
                                           cLogger.log('Importers.index', 'error from image service - ' + err);
                                           status = 500;
                                           options.errorCode = -1;
                                           options.errorMessage = err;
                                         }
                                         cLogger.log('Importers.index', 'invoking callback with status - ' + status + ', path - ' + that.path);
                                         that.doCallbacks(status, result, options);
                                       });
    return that;
  },

  read: function(id, options) {
    cLogger.log('Importers.read', 'Reading for path - ' + this.path + ', id - ' + id);
    var that = this;
    imageService.importBatchShow(id.replace('$', ''),
                                 { includeImages: false },
                                 function(err, result) {
                                   var status = 200;
                                   if (err) {
                                     cLogger.log('Importers.show', 'error from image service - ' + err);
                                     status = 500;
                                     options.errorCode = -1;
                                     options.errorMessage = err;
                                   }
                                   cLogger.log('Importers.show', 'invoking callback with status - ' + status + ', path - ' + that.path);
                                   var callbackOptions = options ? _.clone(options) : {};
                                   callbackOptions['isInstRef'] = true;
                                   that.doCallbacks(status, result, callbackOptions);
                                 });
    return this;
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
    cLogger.log('Importers.transformRep', 'Doing transform, path - ' + this.path);
    var that = this;
    if (options && _.has(options, 'isInstRef') && options.isInstRef) {
      cLogger.log('Importers.transformRep', 'transforming instance to full form...');
      return this._transformOne(rep);
    }
    else {
      if (_.isArray(rep)) {
        cLogger.log('Importers.transformRep', 'transforming collection...');
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
    }
    return rep;
  },

  _transformOne: function(rep) {
    var newRep = {};
    newRep.id = _.has(rep, 'oid') ? '$' + rep.oid : undefined;
    newRep.import_dir = _.has(rep, 'path') ? rep.path : '';
    newRep.created_at = rep.created_at;
    newRep.started_at = rep.getStartedAt();
    newRep.completed_at  = rep.getCompletedAt();
    newRep.num_to_import = rep.getNumToImport();
    newRep.num_imported  = rep.getNumAttempted();
    newRep.num_success   = rep.getNumSuccess();
    newRep.num_error     = rep.getNumError();
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

  //
  initialize: function(path, options) {
    this.parent(path, options);
    cLogger.log('ImportersImages.initialize', 'Initialized, path - ' + this.path + ', name - ' + this.name + ', instance name - ' + this.instName);
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
  },

  //
  //  index:
  //    Args:
  //      req: The request, S.T. req.params[0] contains the importers instance ID.
  //
  index: function(options) {
    cLogger.log('ImportersImages.index', 'Indexing...');
    options = options || {};
    var that = this;
    var batchId = options.req.params[0];
    imageService.importBatchShow(batchId.replace('$', ''),
                                 { includeImages: true },
                                 function(err, result) {
                                   var status = 200;
                                   if (err) {
                                     cLogger.log('ImportersImages.index', 'error from image service - ' + err);
                                     status = 500;
                                     options.errorCode = -1;
                                     options.errorMessage = err;
                                   }
                                   cLogger.log('ImportersImages.index', 'invoking callback with status - ' + status + ', path - ' + that.path);
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
    cLogger.log('ImportersImages.transformRep', 'Doing transform...');
    var newRep = this.importersResource.transformRep(rep, { isInstRef: true,
                                                            resourceName: 'importer' });
    if (newRep) {
      if (rep.images) {
        cLogger.log('ImportersImages.transformRep', 'Doing image transorm - ' + JSON.stringify(rep.images));
        newRep.images = this.imagesResource.transformRep(rep.images,
                                                         { isInstRef: false,
                                                           resourceName: 'images' } );
      }
      else {
        newRep.images = [];
      }
    }
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
    cLogger.log('StorageSynchronizers.initialize', 'Initialized, path - ' + this.path + ', name - ' + this.name + ', instance name - ' + this.instName);
  },

  create: function(attr, options) {
    cLogger.log('StorageSynchronizers.create', 'Payload - ' + JSON.stringify(attr));
    var that = this;
    options.isInstRef = true;
    var synchronizer = undefined;
    try {
      synchronizer = storage.sync();
      function publishSyncEvent(event, synchronizer) {
        notifications.publish('/storage/synchronizers',
                              event,
                              that.transformRep(synchronizer));
      };
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
      cLogger.log('StorageSynchronizers.create', 'Caught error - ' + err);
      options.errorCode = -1;
      options.errorMessage = err.message;
      try {

        that.doCallbacks(500, synchronizer, options);
      }
      catch (err) {
        that.doCallbacks(500, undefined, options);
      }
    }
  },

  read: function(id, options) {
    cLogger.log('StorageSynchronizers.read', 'Reading for path - ' + this.path + ', id - ' + id);
    var that = this;
    options.isInstRef = true;
    var synchronizerId = id.replace('$', '');
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
    cLogger.log('StorageSynchronizers.transformRep', 'Doing transform, path - ' + this.path + ', id - ' + rep.id + ', state - ' + rep.state);
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
//    Note, this is NOT currently exposed as a public API. But, the intent is to use this
//    resource internally, such that access can be gained to document resource and
//    publish to the notifications API when a document changes. Hence, ONLY the
//    create method is implemented, which instantiates a storage.changesFeed instance,
//    and listens for change events which inturn are passed to the notifications API.
//
var StorageChangesFeed = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    cLogger.log('StorageChangesFeed.initialize', 'Initialized, path - ' + this.path + ', name - ' + this.name + ', instance name - ' + this.instName);
  },

  create: function(attr, options) {
    cLogger.log('StorageChangesFeed.create', 'Payload - ' + JSON.stringify(attr) + ', options - ' + options);
    var that = this;

    var since = (options && _.has(options, 'query') && _.has(options.query, 'since')) ? options.query.since : undefined;
    var changesFeed = undefined;

    try {
      changesFeed = storage.changesFeed({since: since});
      function publishImagesChangeEvent(event) {

        cLogger.log('StorageChangesFeed.create', 'About to publish image change event, image doc - ' + JSON.stringify(event.doc));

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
          imageService.show(event.doc.orig_id, function(err, result) {
            if (err) {
              cLogger.log('StorageChangesFeed.create', 'Failed to fetch origin image in order to publish change event, error - ' + err);
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
      };
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
      changesFeed.listen();
      that.doCallbacks(200, changesFeed, options);
    }
    catch (err) {
      cLogger.log('StorageChangesFeed.create', 'Caught error - ' + err);
      options.errorCode = -1;
      options.errorMessage = err.message;
      try {
        that.doCallbacks(500, changesFeed, options);
      }
      catch (err) {
        that.doCallbacks(500, undefined, options);
      }      
    }
  }

});
