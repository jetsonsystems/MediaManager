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
var version = '0.0.1';
var urlVersion = 'v0';
var serverName = 'media_manager_api_server';

var logDir = '/var/log/' + serverName;
var infoLogfile = serverName + '.log';
var errorLogfile = serverName + '.log';
var _ = require('underscore');
var url = require('url');
var restify = require('restify');
var async = require('async');

exports.startServer = startServer;
exports.stopServer = stopServer;

//var serverPort = argv._[0];

var server = null;

function startServer(serverPort, config, callback) {

  var mmApi = require('./lib/MediaManagerApiCore')(config);

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

  server = restify.createServer({name: serverName,
    version: version});

  server.use(restify.bodyParser({ mapParams: false }));

//
//  MediaManagerApiRouter: Sets up routing to resources for the Media Manager API.
//
  var MediaManagerApiRouter = function () {

    //
    //  initialize: sets up all the routes. Invoked at the end of object construction.
    //
    this.initialize = function () {
      var that = this;
      console.log('MediaManagerApiRouter.initialize: initializing...');
      _.each(_.values(this.resources), function (resource) {

        //
        //  Collection routes:
        //

        //
        //  create route (POST resource.path)
        //
        var pat = resource.requestPath('create');
        console.log('MediaManagerApiRouter.initialize: create, request path to match - ' + pat);
        server.post(pat,
          function create(req, res, next) {
            logger.info({
              event: '__request__',
              req: req
            });
            var options = {
              onSuccess: that.genOnSuccess(resource, req, res),
              onError: that.genOnError(resource, req, res)
            };
            var parsedUrl = url.parse(req.url, true);
            if (_.has(parsedUrl, 'query')) {
              options['query'] = parsedUrl.query;
            }
            if (_.has(req, 'body') && req.body) {
              options.attr = req.body;
            }
            resource.doRequest('POST',
              options);
            return next();
          });


        //
        //  update route (PUT resource.path)
        //
        pat = resource.requestPath('update');
        console.log('MediaManagerApiRouter.initialize: update, request path to match - ' + pat);
        server.put(pat,
          function update(req, res, next) {
            logger.info({
              event: '__request__',
              req: req
            });
            var options = {
              id: req.params[0],
              req: req,
              onSuccess: that.genOnSuccess(resource, req, res),
              onError: that.genOnError(resource, req, res)
            };
            var parsedUrl = url.parse(req.url, true);
            if (_.has(parsedUrl, 'query')) {
              options['query'] = parsedUrl.query;
            }
            if (_.has(req, 'body') && req.body) {
              options.attr = req.body;
            }
            resource.doRequest('PUT',
              options);
            return next();
          });

        //
        //  index route (GET resource.path)
        //
        pat = resource.requestPath('index');
        console.log('MediaManagerApiRouter.initialize: index, request path to match - ' + pat);
        server.get(pat,
          function (req, res, next) {
            logger.info({
              event: '__request__',
              req: req});
            var options = {
              req: req,
              onSuccess: that.genOnSuccess(resource, req, res),
              onError: that.genOnError(resource, req, res)
            };
            var parsedUrl = url.parse(req.url, true);
            if (_.has(parsedUrl, 'query')) {
              options['query'] = parsedUrl.query;
            }
            resource.doRequest('GET',
              options);
            return next();
          });

        //
        //  Singular instance routes:
        //

        //
        //  read route (GET resource.path, where resource.path points to an instance)
        //
        pat = resource.requestPath('read');
        console.log('MediaManagerApiRouter.initialize: read, request path to match - ' + pat);
        server.get(pat,
          function (req, res, next) {
            logger.info({event: '__request__',
              id: req.params[0],
              req: req});
            resource.doRequest('GET',
              {id: req.params[0],
                onSuccess: that.genOnSuccess(resource, req, res),
                onError: that.genOnError(resource, req, res)});
            return next();
          });
        console.log('MediaManagerApiRouter.initialize: read defined!');

        //
        //  delete a single instance . route (DELETE resource.path)
        //
        pat = resource.requestPath('delete');
        console.log('MediaManagerApiRouter.initialize: delete, request path to match - ' + pat);
        server.del(pat,
          function (req, res, next) {
            logger.info({
              event: '__request__',
              req: req});
            var options = {
              id: req.params[0],
              req: req,
              onSuccess: that.genOnSuccess(resource, req, res),
              onError: that.genOnError(resource, req, res)
            };
            var parsedUrl = url.parse(req.url, true);
            if (_.has(parsedUrl, 'query')) {
              options['query'] = parsedUrl.query;
            }
            resource.doRequest('DELETE',
              options);
            return next();
          });

        //
        //  delete a collection. route (DELETE resource.path)
        //
        pat = resource.requestPath('collection');
        console.log('MediaManagerApiRouter.initialize: delete a collection, request path to match - ' + pat);
        server.del(pat,
          function (req, res, next) {
            logger.info({
              event: '__request__',
              req: req});
            var options = {
              req: req,
              onSuccess: that.genOnSuccess(resource, req, res),
              onError: that.genOnError(resource, req, res)
            };
            var parsedUrl = url.parse(req.url, true);
            if (_.has(parsedUrl, 'query')) {
              options['query'] = parsedUrl.query;
            }
            resource.doRequest('DELETE',
              options);
            return next();
          });
      });
    };

    console.log('MediaManagerApiRouter.constructor: Setting up resources, typeof(mmApi) === ' + typeof(mmApi) + ', mmApi attributes - ' + _.keys(mmApi));

    this.resources = {
      Images: new mmApi.Images('/images',
        {instName: 'image',
          pathPrefix: '/' + urlVersion}),
      Importers: new mmApi.Importers('/importers',
        {instName: 'importer',
          pathPrefix: '/' + urlVersion}),
      ImportersImages: new mmApi.ImportersImages(null,
        {pathPrefix: '/' + urlVersion,
          subResource: new mmApi.Importers(
            '/importers',
            {instName: 'importer',
              subResource: new mmApi.Images(
                '/images',
                {instName: 'image'})
            })
        }),
      StorageSynchronizers: new mmApi.StorageSynchronizers('/storage/synchronizers',
        {instName: 'synchronizer',
          pathPrefix: '/' + urlVersion }),
      Tags:new mmApi.Tags('/tags',
        {instName:'tag',
          pathPrefix:'/' + urlVersion}),
      Tagger:new mmApi.Tagger('/tagger',
        {instName:'tagger',
          pathPrefix:'/' + urlVersion})
    };

    this.genOnSuccess = function (resource, req, res) {
      return function (responseBody) {
        console.log('index.js: Handling - ' + req.method + ' ' + req.url + ', response payload of length - ' + JSON.stringify(responseBody).length);
        logger.info({event: '__resposne__',
          status: 0});
        res.json(200, responseBody);
      };
    };

    this.genOnError = function (resource, req, res) {
      return function (responseBody) {
        console.log('index.js: Handling - ' + req.method + ' ' + resource.path + ', response payload - ' + JSON.stringify(responseBody));
        var fields = {event: '__response__',
          status: 1,
          error_code: _.has(responseBody, 'error_code') ? responseBody.error_code : -1,
          error_message: _.has(responseBody, 'error_message') ? responseBody.error_message : ""};
        logger.info(fields);
        res.json(500, responseBody);
      };
    };

    this.initialize();
  };

  var mediaManagerApiRouter = new MediaManagerApiRouter();

  async.waterfall(
    [
      function (next) {
        server.listen(serverPort, function () {
          logger.info({event: '__start__',
            listening: serverPort});
          next();
        });

      }
    ],
    function (err, result) {
      if (err) {
        console.log('Unable to start server: ', err);
        callback(err);
      }
      callback(null, result);
    }
  );

}


function stopServer(callback) {
  try {
    server.close(callback);
  } catch (e) {
    console.error(e.stack);
    process.exit(1);
  }
};