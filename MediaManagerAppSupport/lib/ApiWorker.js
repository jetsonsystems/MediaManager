//
// ApiWorker: An individual webworker.
//
var util = require('util');
var fs = require('fs');
var _ = require('underscore');

var osxFs = require('MediaManagerAppSupport/lib/OSXFileSystem');

var log4jsConfigFile = undefined;
if (_.has(process.env, 'LOG4JS_CONFIG')) {
  log4jsConfigFile = process.env.LOG4JS_CONFIG;
}
else {
  log4jsConfigFile = path.join(osxFs.appBundleConfigDir, 'log4js.json');
}
if (!fs.existsSync(log4jsConfigFile)) {
  throw new Error('Unable to find log4js configuration file.');
}

var log4js = require('log4js');
log4js.configure(log4jsConfigFile, { cwd: osxFs.libAppSupportLogDir });

var notifications = require('MediaManagerApi/lib/Notifications');

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
  logger.debug(logPrefix + 'Worker received notification - ' + util.inspect(msg));
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
      logger.debug(lp + 'Request successful!');
      postMessage(ApiWorkerMessages.createRestResponseMsg(request.requestId, 0, responseBody));
      logger.debug(lp + 'Posted responses!');
    };

    options.onError = function(responseBody) {
      logger.debug(lp + 'Request error!');
      postMessage(ApiWorkerMessages.createRestResponseMsg(request.requestId,
                                                          responseBody.status,
                                                          responseBody));
      logger.debug(lp + 'Request error, posted response!');
    };

    resource.doRequest(request.method, options);
  }
  else {
    logger.error(lp + 'Invalid message received by worker, type - ' + e.data.type);
  }
};
