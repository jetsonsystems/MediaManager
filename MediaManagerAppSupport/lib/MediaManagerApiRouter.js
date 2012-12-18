//
//  PLM Media Manager API Router:
//
//    Routing for media manager API endpoints.
//

var _ = require('underscore');
var url = require('url');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore');

//
//  MediaManagerApiRouter: Sets up routing to resources for the Media Manager API.
//
var MediaManagerApiRouter = function(router) {

  //
  //  initialize: sets up all the routes. Invoked at the end of object construction.
  //
  var initialize = function() {
    var that = this;
    console.log('index.js:MediaManagerApiRouter.initialize: initializing...');
    _.each(_.values(this.resources), function(resource) {

      //
      //  Collection routes:
      //
      router.route(resource.requestPath('collection'), {
        //
        //  create route (POST resource.path)
        //
        POST: function(req, res) {
          var options = {
            onSuccess: that.genOnSuccess(resource, req, res),
            onError: that.genOnError(resource, req, res)
          };
          var parsedUrl = url.parse(req.originalUrl, true);
          if (_.has(parsedUrl, 'query')) {
            options['query'] = parsedUrl.query;
          }
          //
          //  Need to populate attr with the request body.
          //
          try {
            // options.attr = JSON.parse(req.payload);
            options.attr = req.data;
          }
          catch (err) {
            //
            // Nuke attr to indicate we do NOT have a valid JSON payload.
            //
            options.attr = undefined;
            delete options.attr;
          }
          resource.doRequest('POST',
                             options);
        },
        //
        //  index route (GET resource.path)
        //
        GET: function(req, res) {
          var options = {
            onSuccess: that.genOnSuccess(resource, req, res),
            onError: that.genOnError(resource, req, res)
          };
          var parsedUrl = url.parse(req.originalUrl, true);
          if (_.has(parsedUrl, 'query')) {
            options['query'] = parsedUrl.query;
          }
          resource.doRequest('GET',
                            options);
        }
      });
      //
      //  Singular instance routes:
      //
      router.route(resource.requestPath('instance'), {
        //
        //  read route (GET resource.path, where resource.path points to an instance)
        //
        GET: function(req, res) {
          resource.doRequest('GET',
                             {id: req.params[0],
                              onSuccess: that.genOnSuccess(resource, req, res),
                              onError: that.genOnError(resource, req, res)});
        }
      });
    });
  };

  var pathPrefix = '/api/media-manager/v0';

  this.resources = {

    Images: new mmApi.Images('/images', 
                             {instName: 'image',
                              pathPrefix: pathPrefix}),
    Importers: new mmApi.Importers('/importers', 
                                   {instName: 'importer',
                                    pathPrefix: pathPrefix}),
    ImportersImages: new mmApi.ImportersImages(null,
                                               {pathPrefix: pathPrefix,
                                                subResource: new mmApi.Importers(
                                                  '/importers', 
                                                  {instName: 'importer',
                                                   subResource: new mmApi.Images(
                                                     '/images', 
                                                     {instName: 'image'})
                                                  })
                                               })
  };

  this.genOnSuccess = function(resource, req, res) {
    return function(responseBody) {
      console.log('index.js: Handling - ' + req.method + ' ' + resource.path + ', response payload of length - ' + JSON.stringify(responseBody).length);
      res.send(200,
               'application/json',
               JSON.stringify(responseBody));
    };
  };

  this.genOnError = function(resource, req, res) {
    return function(responseBody) {
      console.log('index.js: Handling - ' + req.method + ' ' + resource.path + ', response payload - ' + JSON.stringify(responseBody));
      res.send(responseBody.status,
               'application/json',
               JSON.stringify(responseBody));
    };
  };

  initialize.apply(this);
};

module.exports = MediaManagerApiRouter;
