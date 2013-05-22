//
// AppRestService:
//
var version = '0.0.1';
var serverName = 'app_rest_service';

var util = require('util');
var path = require('path');
var _ = require('underscore');
var log4js = require('log4js');
var url = require('url');
var restify = require('restify');

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var fname = tmp[tmp.length-1];
var logPrefix = fname.replace('.js', ': ');

var config = require('MediaManagerAppConfig');
var apiResources = require('MediaManagerAppSupport/lib/ApiResources');
var apiWorkers = require('MediaManagerAppSupport/lib/ApiWorkers');

var serverPort = config.services.restAPI.port;

logger.info(logPrefix + '__info__ Starting REST service, host - ' + config.services.restAPI.host + ', port - ' + serverPort);

var server = restify.createServer({name: serverName,
                                   version: version});

server.use(restify.bodyParser({ mapParams: false }));

//
// Router: Our request router.
//
var Router = function() {

  this.initialize = function() {
    var that = this;

    var lp = fname.replace('.js', '.Router.initialize: ');

    logger.debug(lp + '__info__ Initializing...');
    _.each(_.values(apiResources), function(resource) {

      //
      //  Collection routes:
      //

      //
      //  create route (POST resource.path)
      //
      var pat = resource.requestPath('create');
      logger.debug(lp + '__info__ Router configuration; create, request path to match - ' + pat);

      server.opts(pat,
                  function(req, res, next) {
                    res.header('Access-Control-Allow-Origin', 'http://appjs');
                    res.header('Access-Control-Allow-Methods', "POST, GET, OPTIONS");
                    res.header('Access-Control-Allow-Headers', 'Content-Type');
                    res.send(204);
                    return next();
                  });

      server.post(pat,
                  function create(req, res, next) {
                    logger.info(lp + '__request__ req - ' + util.inspect(req));
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
                    res.header('Access-Control-Allow-Origin', 'http://appjs');
                    res.header('Access-Control-Allow-Methods', "POST, GET, OPTIONS");
                    apiWorkers.doRequest(resource, 'POST', options);
                    return next();
                  });

      //
      //  index route (GET resource.path)
      //
      pat = resource.requestPath('index');
      logger.debug(lp + '__info__ Router configuration; index, request path to match - ' + pat);
      server.get(pat,
                 function(req, res, next) {
                   logger.info(lp + '__request__ req - ' + util.inspect(req));
                   var options = {
                     req: that._reqOption(req),
                     onSuccess: that.genOnSuccess(resource, req, res),
                     onError: that.genOnError(resource, req, res)
                   };
                   var parsedUrl = url.parse(req.url, true);
                   if (_.has(parsedUrl, 'query')) {
                     options['query'] = parsedUrl.query;
                   }
                   res.header('Access-Control-Allow-Origin', 'http://appjs');
                   res.header('Access-Control-Allow-Methods', "GET, OPTIONS");
                   apiWorkers.doRequest(resource, 'GET', options);
                   return next();
                 });

      //
      //  Singular instance routes:
      //

      pat = resource.requestPath('read');
      server.opts(pat,
                  function(req, res, next) {
                    res.header('Access-Control-Allow-Origin', 'http://appjs');
                    res.header('Access-Control-Allow-Methods', "GET, PUT, OPTIONS");
                    res.header('Access-Control-Allow-Headers', 'Content-Type');
                    res.send(204);
                    return next();
                  });

      //
      //  read route (GET resource.path, where resource.path points to an instance)
      //
      logger.debug(lp + '__info__ Router configuration; read, request path to match - ' + pat);
      server.get(pat,
                 function(req, res, next) {
                   logger.info(lp + '__request__ id - ' + req.params[0] + ', req - ' + util.inspect(req));
                   res.header('Access-Control-Allow-Origin', 'http://appjs');
                   res.header('Access-Control-Allow-Methods', "POST, GET, PUT, OPTIONS");
                   apiWorkers.doRequest(resource, 'GET',
                                        {id: req.params[0],
                                         onSuccess: that.genOnSuccess(resource, req, res),
                                         onError: that.genOnError(resource, req, res)});
                   return next();
                 });

      //
      //  update route (PUT resource.path, where resource.path points to an instance)
      //
      pat = resource.requestPath('update');
      logger.debug(lp + '__info__ Router configuration; update, request path to match - ' + pat);
      server.put(pat,
                 function(req, res, next) {
                   logger.info(lp + '__request__ id - ' + req.params[0] + ', req - ' + util.inspect(req));
                   res.header('Access-Control-Allow-Origin', 'http://appjs');
                   res.header('Access-Control-Allow-Methods', "PUT");
                   var options = {
                     id: req.params[0],
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
                   apiWorkers.doRequest(resource, 'PUT', options);
                   return next();
                 });
    });
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

  this._reqOption = function(req) {
    var reqOpt = {};

    if (_.has(req, 'params')) {
      reqOpt.params = req.params;
    }
    return reqOpt;
  };

  this.initialize();
};

var router = new Router();

server.listen(serverPort, function() {
  logger.info(logPrefix + '__start__ ' + serverName + ', listening on port - ' + serverPort);
});

exports.shutdown = function() {
  logger.info(logPrefix + '__shutdown__ ' + serverName + ', shutting down...');
  apiWorkers.shutdown();
  server.close();
};
