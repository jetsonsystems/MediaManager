//
// ApiResources: Collection of API resources.
//

var path = require('path');
var config = require('MediaManagerAppConfig');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore')(config);
var apiVersion = config.services.restAPI.version;
var pathPrefix = path.join(config.services.restAPI.pathPrefix, apiVersion);

module.exports = {

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
