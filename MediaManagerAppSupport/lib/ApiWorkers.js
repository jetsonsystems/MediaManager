//
// ApiWorkers: Manages a collection of WebWorkers. There is a WebWorker for each REST API endpoint.
//
//  Exposes the following method to send a REST API requeset to a WebWorker:
//
//    doRequest(resource, method, options):
//
//  For definitions of messages exchanged, see ApiWorkerMessages.
//
var path = require('path');
var util = require('util');
var _ = require('underscore');
var log4js = require('log4js');
var Worker = require('webworker');

var notifications = require('MediaManagerApi/lib/Notifications');
var ApiWorkerMessages = require('./ApiWorkerMessages');
var ApiResources = require('./ApiResources');

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

//
// requests: <requestId> -> <request>
// 
//  <request> ::= { 
//    requestId: <id>, 
//    resource: <resouce>, 
//    options: <options> 
//  }
//
//    <options> ::= original options passed into doRequest which would include
//      any callbacks, for example.
//
requests = {};

//
// workers: A set of workers. There is ONE worker for each API
//   endpoint. workers is a mapping: resource.fullPath() -> worker.
//
var workers = {};

_.each(_.values(ApiResources), function(resource) {
  var workerPath = path.join(__dirname, 'ApiWorker.js');
  var worker = new Worker(workerPath);
  workers[resource.fullPath()] = worker;

  logger.info(logPrefix + 'worker created for resource - ' + resource.fullPath() + ', worker path - ' + workerPath);

  //
  // onmessage: A message from the worker. Messages from a worker come in 2 flavors -
  //  1. A response to a request was generated. Any callbacks associated with
  //    the request must be invoked with the response data.
  //  2. A notifications was published. The notifications must be published to
  //    the notifications message bus in this context.
  //
  worker.onmessage = function(e) {
    if (e.data.type === ApiWorkerMessages.REST_RESPONSE) {
      var response = e.data.response;
      var requestId = response.requestId;
      logger.info(logPrefix + 'Worker responded for request - ' + requestId);

      if (_.has(requests, requestId)) {
        var options = requests[requestId].options;
        if ((response.status === 0) && _.has(options, "onSuccess")) {
          options.onSuccess(response.body);
        }
        if ((response.status !== 0) && _.has(options, "onError")) {
          options.onError(response.body);
        }
      }
      else {
        logger.error(logPrefix + 'Request not found w/ id - ' + requestId);
      }
    }
    else if (e.data.type === ApiWorkerMessages.NOTIFICATION) {
      var notification = e.data.notification;
      logger.debug(logPrefix + 'Received notification message - ' + util.inspect(notification));
      notifications.publish(notification.resource,
                            notification.event,
                            notification.data);
    }
    else {
      logger.error(logPrefix + 'Invalid message type received from worker, type - ' + e.data.type);
    }
  };

  worker.onexit = function(c, s) {
    logger.info(logPrefix + 'worker exited, code - ' + c + ', signal - ' + s);
  };

});

//
// doRequest: Make a request.
//
//  Args:
//    resource: REST api resrouce instance. See ApiResources.
//    method: GET | POST | PUT | DELETE
//    options: Request options. MUST be JSON or string rep. parsable as JSON. Fields
//      include attr, onSuccess, onError. See MediaManagerApiCore doc.....
//
module.exports.doRequest = function(resource,
                                    method,
                                    options) {
  var lp = logPrefix.replace('.js: ', '.doRequest: ');
  options = _.isString(options) ? JSON.parse(options) : options;
  logger.debug(lp + 'request to resource - ' + resource.fullPath() + ', method - ' + method + ", options - " + util.inspect(options));

  options = options || {};
  var worker = workers[resource.fullPath()];

  logger.info(lp + 'Got worker... - ' + worker.toString());

  var requestOpts = _.omit(options, 'onSuccess', 'onError');

  logger.info(lp + 'Request opts - ' + requestOpts + ', about to create msg - ' + ApiWorkerMessages.toString());

  var msg = ApiWorkerMessages.createRestRequestMsg(resource, method, requestOpts);

  logger.debug(lp + 'Created msg - ' + util.inspect(msg));

  var request = _.clone(msg.request);
  request.options = options;
  requests[request.requestId] = request;

  logger.info(lp + 'Posting message to worker - ' + worker.toString() + ', message - ' + JSON.stringify(msg));

  worker.postMessage(msg);

  logger.info(lp + 'Posted message to worker...');

};

module.exports.shutdown = function() {
  var lp = logPrefix.replace('.js: ', '.shutdown: ');
  logger.info(lp + 'Shuting down...');
};

