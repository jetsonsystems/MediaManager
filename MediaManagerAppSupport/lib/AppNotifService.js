//
// AppNotifService:
//
var path = require('path');
var _ = require('underscore');
var log4js = require('log4js');

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

var WebSocketServer = require('websocket').server;
var http = require('http');
var config = require('MediaManagerAppConfig');
var notifications = require('MediaManagerApi/lib/Notifications');

var serverPort = config.services.notifAPI.port;

logger.info(logPrefix + 'Starting Notifications service, host - ' + config.services.notifAPI.host + ', port - ' + serverPort);

var server = http.createServer(function(request, response) {
  logger.info(logPrefix + '__request__ Received request, url - ' + request.url);

  response.writeHead(404);
  response.end();
});

server.listen(serverPort, function() {
  logger.info(logPrefix + '__start__ Server listening on port - ' + serverPort);
});

var wsServer = new WebSocketServer({
  httpServer: server,
  autoAcceptConnections: false
});

function originIsAllowed(origin) {
  return true;
};

//
// MessageHandler: Handle messages with respect to this connection.
//
var MessageHandler = function(connection) {
  var that = this;

  this.connection = connection;
  //
  //  All subscriptions shared by this connection.
  //
  this.subscriptions = {};

  this.handle = function(message) {
    logger.debug(logPrefix + 'Message  keys - ' + _.keys(message));
    if ((message.resource === '_client') && (message.event === 'subscribe')) {
      if (_.has(message.data, 'resource')) {
        _handleSubscription(message.data.resource);
      }
      else {
        logger.error(logPrefix + '__message__ Invalid message format - ' + JSON.stringify(message));
      }
    }
    else {
      logger.error(logPrefix + '__message__ Invalid message - ' + message);
    }
  };

  var _handleSubscription = function(resource) {
    if (_.has(that.subscriptions, resource)) {
      logger.info(logPrefix + '__subscription__ Subscription to resource events already exists, resource - ' + resource);
    }
    else {
      logger.info(logPrefix + '__subscription__ Adding subscription to resource - ' + resource);
      function handleCallbacks(msg) {
        logger.info(logPrefix + '__subscription__ handling subscription callback, msg - ' + JSON.stringify(msg));
        that.connection.sendUTF(JSON.stringify(msg));
      }
      var subscription = notifications.subscribe(resource, handleCallbacks);
      that.subscriptions[resource] = subscription;
      logger.info(logPrefix + '__subscription__ Added subscription to resource - ' + resource);
    }
  };

};

wsServer.on('request', function(request) {

  if (!originIsAllowed(request.origin)) {
    request.reject();
    logger.info(logPrefix + '__rejected__ Request rejected, origin - ' + request.origin);
    return ;
  }

  var connection = request.accept(null, request.origin);

  connection.sendUTF(JSON.stringify({resource: '/notifications',
                                     event: 'connection.established' }));

  var messageHandler = new MessageHandler(connection);

  connection.on('message', function(message) {
    if (message.type === 'utf8') {
      logger.info(logPrefix + '__message__ Received message - ' + message.utf8Data);
      messageHandler.handle(JSON.parse(message.utf8Data));
    }
    else {
      logger.info(logPrefix + '__message__ Received unsupported message type.');
    }
  });

  connection.on('frame', function() {
    logger.info(logPrefix + '__frame__ Received frame...');
  });

  connection.on('error', function() {
    logger.info(logPrefix + '__error__ Connection error!');
  });

  connection.on('close', function(reasonCode, description) {
    logger.info(logPrefix + '__close__ Connection closed, reason - ' + reasonCode);
  });

});

exports.shutdown = function() {
  logger.info(logPrefix + '__shutdown__ Shutting down...');
};
