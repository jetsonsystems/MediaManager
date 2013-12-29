//
// resource: A REST resource to be used as a building blog for api elements.
//

var util = require('util');
var _ = require('underscore');
require('mootools');
var log4js = require('log4js');

var log = log4js.getLogger('plm.MediaManagerApi');

module.exports = function(errorCodes, errors) {

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
      log.debug('Resource.doCallbacks: w/ status - %s, path - %s', status, this.path);
      if (result) {
        options.rep = result;
      }
      if (status === 200) {
        log.debug('Resource.doCallbacks: 200 response code...');
        if (_.has(options, 'onSuccess') && options.onSuccess) {
          log.debug('Resource.doCallbacks: creating response body...');
          var rb = this.createResponseBody(0, options);
          log.debug('Resource.doCallbacks: response body of - ' + JSON.stringify(rb));
          options.onSuccess(rb);
        }
        else {
          log.debug('Resource.doCallbacks: no on success method...');
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
      log.debug('Resource.createResponseBody: Creating response body w/ status - %s', status);
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

  return Resource;

};
