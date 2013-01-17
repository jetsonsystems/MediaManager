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

var opts = require('optimist')
  .boolean('v')
  .boolean('c')
  .usage('Usage: $0 [<options>] <port>\n\nMedia Managager API Server.')
  .options({
    'c' : {
      'alias' : 'config',
      'default' : false,
      'describe' : 'Use MediaManageAppConfig instead of -d, -h, and/or -p options.'
    },
    'd' : {
      'alias' : 'dbname',
      'default' : 'plm-media-manager',
      'describe' : 'Database name.'
    },
    'h' : {
      'alias' : 'dbhost',
      'default' : 'localhost',
      'describe' : 'TouchDB / CouchDB host.'
    },
    'p' : {
      'alias' : 'dbport',
      'default' : 5984,
      'describe' : 'TouchDB / CouchDB port number.'
    }
  });
var argv = opts.argv;

var argsOk = function(argv) {
  if (argv._.length !== 1) {
    return false;
  }
  return true;
}(argv);

if (!argsOk) {
  opts.showHelp();
  process.exit(1);
}

var serverPort = argv._[0];

var config = undefined;

if (argv.c) {
  config = require('MediaManagerAppConfig');
}
else {
  config = {
    db: {
      database: argv.d,
      local: {
        host: argv.h,
        port: argv.p
      }
    }
  };
}

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

var server = restify.createServer({name: serverName,
                                   version: version});

server.use(restify.bodyParser({ mapParams: false }));

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
      //  index route (GET resource.path)
      //
      pat = resource.requestPath('index');
      console.log('MediaManagerApiRouter.initialize: index, request path to match - ' + pat);
      server.get(pat,
                 function(req, res, next) {
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
                 function(req, res, next) {
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
                                                          pathPrefix: '/' + urlVersion })
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
