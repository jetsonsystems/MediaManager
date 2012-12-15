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
// config: configuration function. 
//    Note, perhaps this should be an object which when values change we
//    update values in the image service.
//
//    options:
//      dbHost
//      dbPort
//      dbName
//
var config = exports.config = function(options) {
  if (_.has(options, 'dbHost')) {
    imageService.config.db.host = options.dbHost;
  }
  if (_.has(options, 'dbPort')) {
    imageService.config.db.port = options.dbPort;
  }
  if (_.has(options, 'dbName')) {
    imageService.config.db.name = options.dbName;
  }
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
//        PUTE    resource instance           update
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
  //
  initialize: function(path, options) {
    this.path = path;
    this.name = _.last(path.split('/'));
    this.instName = options && _.has(options, 'instName') ? options.instName : this.name;
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
var Images = exports.Images = new Class({

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
      depth: 'depth',
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
    if (options && _.has(options, 'isInstRef')) {
      cLogger.log('Images.transformRep', 'transforming instance to full form...');
      return that._transformToFullFormRep(rep);
    }
    else {
      if (_.isArray(rep)) {
        cLogger.log('Images.transformRep', 'transforming collection to array of short forms...');
        cLogger.log('Images.transformRep', 'type of - ' + typeof(that._shortFormAttrs) + ', desired short form attributes - ' + JSON.stringify(_.values(that._shortFormAttrs)));
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
var Importers = exports.Importers = new Class({

  Extends: Resource,

  initialize: function(path, options) {
    this.parent(path, options);
    cLogger.log('Importers.initialize', 'Initialized, path - ' + this.path + ', name - ' + this.name + ', instance name - ' + this.instName);
  },

  create: function(attr, options) {
    cLogger.log('Importers.create', 'Payload - ' + JSON.stringify(attr));
    var that = this;
    options.isInstRef = true;
    var importDir = (attr && _.has(attr, 'import_dir')) ? attr.import_dir : undefined;
    if (importDir) {
      try {
        var importOptions = {
          recursionDepth: (_.has(options, 'query') && _.has(options.query, 'dive') && (options.query.dive === 'false')) ? 1 : 0,
          saveOriginal: false,
          desiredVariants: [{ name: 'thumbnail.jpg', format: 'jpg', width: 80, height: 80}, 
                            { name: 'web.jpg', format: 'jpg', width: 640, height: 400}, 
                            { name: 'full-small.jpg', format: 'jpg', width: 1280, height: 800}]
        };
        
        imageService.batchImportFs(
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
    if (options && _.has(options, 'isInstRef')) {
      cLogger.log('Images.transformRep', 'transforming instance to full form...');
      var newRep = {};
      newRep.id = _.has(rep, '_id') ? '$' + rep._id : undefined;
      newRep.import_dir = _.has(rep, 'path') ? rep.path : '';
      newRep.created_at = rep.created_at;
      newRep.started_at = rep.created_at;
      newRep.completed_at = _.has(rep, 'ended_at')? rep.ended_at : undefined;
      newRep.num_to_import = rep.num_to_import;
      newRep.num_imported = rep.num_imported;
      newRep.num_success = rep.num_success;
      newRep.num_error = rep.num_error;
      return newRep;
    }
    return rep;
  },

});

config({dbHost: 'localhost',
        dbPort: 5984,
        dbName: 'plm-media-manager-test0'});
