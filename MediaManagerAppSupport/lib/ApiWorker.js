//
// ApiWorker: An individual webworker.
//
var util = require('util');
var _ = require('underscore');
var log4js = require('log4js');

var notifications = require('MediaManagerApi/lib/Notifications');

var configPath = '/Users/marekjulian/Projects/PLM/DeskTopApp/node-webworker/test/config/log4js.json';
log4js.configure(configPath);
var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

logger.info(logPrefix + 'Initializing resources...');

var ApiWorkerMessages = require('MediaManagerAppSupport/lib/ApiWorkerMessages');
var ApiResources = require('MediaManagerAppSupport/lib/ApiResources');
var resourcesByPath = {};

_.each(_.values(ApiResources), function(resource) {
  logger.info(logPrefix + 'Adding resource w/ path - ' + resource.fullPath());
  resourcesByPath[resource.fullPath()] = resource;
});

//
// Subscribe to notifications which are posted by the various resources.
// Getting a notification callback will result in posting a notification
// message to the ApiWorkers module.
//
function handleNotification(msg) {
  logger.info(logPrefix + 'Worker received notification - ' + util.inspect(msg));
  postMessage(ApiWorkerMessages.createNotificationMsg(msg));
};

notifications.subscribe('/importers', handleNotification);
notifications.subscribe('/storage/synchronizers', handleNotification);
notifications.subscribe('/storage/changes-feed', handleNotification);

//
// onmessage: Process a message, which should describe the request to handle.
//  The message should be of type ApiWorkerMessages.REST_REQUEST
//
onmessage = function(e) {
  var lp = logPrefix.replace(': ', '.onmessage: ');
  if (e.data.type === ApiWorkerMessages.REST_REQUEST) {
    var request = e.data.request;
    logger.info(lp + 'Request, resource - ' + request.resource);
    var resource = resourcesByPath[request.resource];
    var options = request.options || {};

    options.onSuccess = function(responseBody) {
      postMessage(ApiWorkerMessages.createRestResponseMsg(request.requestId, 0, responseBody));
    };

    options.onError = function(responseBody) {
      postMessage(ApiWorkerMessages.createRestResponseMsg(request.requestId,
                                                          responseBody.status,
                                                          responseBody));
    };

    resource.doRequest(request.method, options);
  }
  else {
    logger.info(lp + 'Invalid message received by worker, type - ' + e.data.type);
  }
};
