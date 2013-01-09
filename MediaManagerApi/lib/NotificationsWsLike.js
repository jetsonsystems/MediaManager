//
//  MediaManagerApi/lib/NotificationsWsLike.js: Exports a WebSocket like interface
//    in order to receive notifications as defined here:
//
//    http://projects.jetsonsys.com/projects/plm-media-manager-web-api/wiki/NotificationsApi
//
//    The exported interface is intended to be compatable with the HTML WebSocket interface.
//    See:
//
//    http://dev.w3.org/html5/websockets/#the-websocket-interface
//    http://www.websocket.org/aboutwebsocket.html
//
//    Specifically, the following API is exported:
//
//      Constructor: NotificationsWsLike(url, protocol)
//        * Note, protocal is ignored.
//
//        attributes:
//           URL: Url used to connect.
//           readyState: Read only, possible values:
//            
//             CONNECTING: 0
//             OPEN: 1
//             CLOSED: 2
//
//           bufferedAmount:
//             Note, for compatability. Value is not set, nor used.
//        
//        methods
//          onopen: onopen callback.
//          onmessage: onmessage callback.
//          onclose: onclose callback.

//          boolean send(data): Send data.
//          void close(): Close WebSocket.
//
//    Usage:
//
//      The module exports a single constructor. Hence, in the AppJS client do the
//      following:
//
//      var WebSocket = require('NotificationsWsLike');
//
//      var ws = new WebSocket('ws://appjs');
//
//      Notes:
//        * the protocal must be 'ws' or 'wss'.
//        * the host MUST be 'appjs'.
//

var notifications = require('Notifications');

module.exports = function WsLikeConstructor(url, protocal) {

  this.URL = url;

  this.CONNECTING = 0;
  this.OPEN = 1;
  this.CLOSED = 2;

  this.onopen = undefined;
  this.onmessage = undefined;
  this.onclose = undefined

  this.send = function(event) {
    var parsedEvent;

    try {
      parsedEvent = JSON.parse('data');
    }
    catch (err) {
      throw Object.create(new Error(),
                          { name: { value: 'EventParseError' },
                            message: { value: 'MediaManagerApi/lib/NotificationsWsLike.send: Unparsable event!' } });
    }

    if (!_.has(parsedEvent, 'resource') || (parsedEvent.resource !== '_client')) {
      throw Object.create(new Error(),
                          { name: { value: 'InvalidSendResource' },
                            message: { value: 'MediaManagerApi/lib/NotificationsWsLike.send: Invalid resource!' } });
    }
    if (!_.has(parsedEvent, 'event')) {
      throw Object.create(new Error(),
                          { name: { value: 'NoEventOnSend' },
                            message: { value: 'MediaManagerApi/lib/NotificationsWsLike.send: Event is required!' } });
    }
    //
    // Have a valid client event to send.
    //
    if (parsedEvent.event === 'subscribe') {
      sendSubscribe.call(this, parsedEvent);
    }
    else if (parsedEvent.event === 'unsubscribe') {
      sendUnsubscribe(parsedEvent);
    }
    else {
      throw Object.create(new Error(),
                          { name: { value: 'InvalidEventOnSend' },
                            message: { value: 'MediaManagerApi/lib/NotificationsWsLike.send: Invalid event - ' + parsedEvent.event + '!' } });
    }
  };

  this.close = function() {};

  //
  //  All subscriptions shared by this instance.
  //
  var subscriptions = {};

  //
  // sendSubscribe: client is sending a 'subscribe' event. 
  //  Subscribe events have the following format:
  //
  //  {
  //    "resource": "_client",
  //    "event": "subscribe",
  //    "data": {
  //      "resource": <resource whose emitted events are being subscribed to>
  //    }
  //  }
  //
  function sendSubscribe(parsedEvent) {
    if (!_.has(subscriptions, parsedEvent.data.resource)) {
      function handleCallbacks(msg) {
        if (this.onmessage) {
          this.onmessage({ data: JSON.stringify(msg) });
        }
      }
      var subscription = notifications.subscribe(parsedEvent.data.resource, handleCallbacks);
      subscriptions[parsedEvent.data.resource] = subscription;
    }
  }

  //
  // sendUnsubscribe: like sendSubscribe.
  //
  function sendUnsubscribe(parsedEvent) {
    if (_.has(subscriptions, parsedEvent.data.resource)) {
      notifications.unsubscribe(subscriptions[parsedEvent.data.resource]);
      delete subscriptions[parsedEvent.data.resource];
    }
  }

 };
