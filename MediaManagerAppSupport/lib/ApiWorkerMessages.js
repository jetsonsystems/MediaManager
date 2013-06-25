//
// ApiWorkerMessages: Shared elements pertaining to messages which are exchanged via the 
//  ApiWorkers module, and spawned WebWorkers.
//
//  The following flavors of messages are passed between the ApiWorkers layer and WebWorkers responsible
//  for servicing requests:
//
//    1. Send an API request to a web-worker:
//
//      var msg = { type: ApiWorkerMessages.REST_REQUEST,
//                  request: {
//                    requestId: requestId,
//                    resource: resource.fullPath(),
//                    method: method,
//                    options: requestOpts
//                  }
//                }
//
//      worker.postMessage(msg);
//
//    2. Message from a WebWorker with the response from a previous request (see 1):
//
//      worker.onmessage = function(msg) { ...
//
//      msg:
//        { type: ApiWorkerMessages.REST_RESPONSE,
//          response: {
//            requestId: requestId,
//            status: <status>,
//            body: <response body>
//          }
//        }
//
//    3. Message from a WebWorker with a notification which was generated:
//
//      worker.onmessage = function(msg) { ...
//
//      msg:
//        { type: ApiWorkerMessages.NOTIFICATION,
//          notification: <body of notification>
//        }
//
var util = require('util');
var uuid = require('node-uuid');
var log4js = require('log4js');
var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

//
// Constants which define a message type.
//
exports.REST_REQUEST = 1;
exports.REST_RESPONSE = 2;
exports.NOTIFICATION = 3;

//
// Factory methods:
//
// createRestRequestMsg:
//
//    { 
//      type: ApiWorkerMessages.REST_REQUEST,
//      request: {
//        requestId: requestId,
//        resource: resource.fullPath(),
//        method: method,
//        options: requestOpts
//      }
//    }
//
exports.createRestRequestMsg = function(resource, method, options) {
  var requestId = uuid.v4();
  logger.info(logPrefix + 'Creating REST request message, id - ' + requestId);
  var msg = {
    type: exports.REST_REQUEST,
    request: {
      requestId: requestId,
      resource: resource.fullPath(),
      method: method,
      options: options 
    }
  };
  logger.debug(logPrefix + 'Created REST request message - ' + util.inspect(msg));
  return msg;
};

//
// createRestResponseMsg:
//
exports.createRestResponseMsg = function(requestId, status, responseBody) {
  return {
    type: exports.REST_RESPONSE,
    response: {
      requestId: requestId,
      status: status,
      body: responseBody
    }
  };
};

//
// createNotificationMsg:
//
exports.createNotificationMsg = function(notification) {
  return {
    type: exports.NOTIFICATION,
    notification: notification
  };
};
