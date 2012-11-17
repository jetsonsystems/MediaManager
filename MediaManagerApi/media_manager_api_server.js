//
//  Media Manager API Server: Implements the media manager APIs which are exposed over HTTP.
//
//    * Current API version is v0.
//    * All request URLs are prefixed with /<version>.
//    * Currently implemented resources are:
//      /images
//    * See the API documentation: http://projects.jetsonsys.com/projects/plm-media-manager-web-api/wiki/Wiki
//      * Note: the /api/media-manager path prefix if required is added by any reverse proxy forwarding requests to this
//        service.
//
var version = '0.0.0';
var urlVersion = 'v0';
var serverName = 'media_manager_api_server';
var serverPort = 9000;
var logDir = '/var/log/' + serverName;
var infoLogfile = serverName + '.log';
var errorLogfile = serverName + '.log';

var _ = require('underscore');
var url = require('url');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore');
var restify = require('restify');

var bunyan = require('bunyan');
var logger = bunyan.createLogger({
  name: serverName,
  streams: [
    {
      level: "info",
      path: logDir + '/' + infoLogfile
    },
    {
      level: "error",
      path: logDir + '/' + errorLogfile
    }
  ]
});

var server = restify.createServer({name: serverName,
                                   version: version});

//
//  MediaManagerApiRouter: Sets up routing to resources for the Media Manager API.
//
var MediaManagerApiRouter = function() {

  //
  //  initialize: sets up all the routes. Invoked at the end of object construction.
  //
  this.initialize = function() {
    var that = this;
    console.log('MediaManagerApiRouter.initialize: initializing...');
    _.each(_.values(this.resources), function(resource) {

      //
      //  Collection routes:
      //

      //
      //  index route (GET resource.path)
      //
      server.get(resource.path,
                 function(req, res) {
                   logger.info({
                     event: '__request__',
                     req: req});
                   var options = {
                     onSuccess: that.genOnSuccess(resource, req, res),
                     onError: that.genOnError(resource, req, res)
                   };
                   var parsedUrl = url.parse(req.url, true);
                   if (_.has(parsedUrl, 'query')) {
                     options['query'] = parsedUrl.query;
                   }
                   resource.doRequest('GET',
                                      options);
                 });

      //
      //  Singular instance routes:
      //

      //
      //  read route (GET resource.path, where resource.path points to an instance)
      //
      server.get(/^\/v0\/images\/(\$[^\/]+)$/,
                 function(req, res) {
                   logger.info({event: '__request__',
                                id: req.params[0],
                                req: req});
                   resource.doRequest('GET',
                                      {id: req.params[0],
                                       onSuccess: that.genOnSuccess(resource, req, res),
                                       onError: that.genOnError(resource, req, res)});
                 });
    });
  };

  this.resources = {
    Images: new mmApi.Images('/' + urlVersion + '/images', 
                             {instName: 'image'})
  };

  this.genOnSuccess = function(resource, req, res) {
    return function(responseBody) {
      console.log('index.js: Handling - ' + req.method + ' ' + req.url + ', response payload of length - ' + JSON.stringify(responseBody).length);
      logger.info({event: '__resposne__',
                   status: 0});
      res.json(200, responseBody);
    };
  };

  this.genOnError = function(resource, req, res) {
    return function(responseBody) {
      console.log('index.js: Handling - ' + req.method + ' ' + resource.path + ', response payload - ' + JSON.stringify(responseBody));
      var fields = {event: '__response__',
                    status: 1,
                    error_code: _.has(responseBody, 'error_code')? responseBody.error_code : -1,
                    error_message: _.has(responseBody, 'error_message')? responseBody.error_message : ""};
      logger.info(fields);
      res.json(500, responseBody);
    };
  };

  this.initialize();
};

var mediaManagerApiRouter = new MediaManagerApiRouter();

server.listen(serverPort, function() {
  logger.info({event: '__start__',
               listening: serverPort});
});
