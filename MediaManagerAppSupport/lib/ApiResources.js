//
// ApiResources: Collection of API resources.
//

var path = require('path');
var config = require('MediaManagerAppConfig');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore')(config);
var apiVersion = config.services.restAPI.version;
var pathPrefix = path.join(config.services.restAPI.pathPrefix, apiVersion);
var urlRewriters = require('./ApiUrlRewriters');

module.exports = {

  Images: new mmApi.Images('/images', 
                           {instName: 'image',
                            pathPrefix: pathPrefix,
                            postTransformHook: urlRewriters.images
                           }),
  Importers: new mmApi.Importers('/importers', 
                                 {instName: 'importer',
                                  pathPrefix: pathPrefix,
                                  postTransformHook: urlRewriters.importers,
                                  imagesPostTransformHook: urlRewriters.images
                                 }),
  ImportersImages: new mmApi.ImportersImages(null,
                                             {pathPrefix: pathPrefix,
                                              subResource: new mmApi.Importers(
                                                '/importers', 
                                                {instName: 'importer',
                                                 subResource: new mmApi.Images(
                                                   '/images', 
                                                   {instName: 'image'})
                                                }),
                                              postTransformHook: urlRewriters.importersImages
                                             }),
  StorageSynchronizers: new mmApi.StorageSynchronizers('/storage/synchronizers',
                                                       {instName: 'synchronizer',
                                                        pathPrefix: pathPrefix}),
  Tags:new mmApi.Tags('/tags',
                      {instName:'tag',
                       pathPrefix: pathPrefix}),
  Tagger:new mmApi.Tagger('/tagger',
                          {instName:'tagger',
                           pathPrefix: pathPrefix})

};
