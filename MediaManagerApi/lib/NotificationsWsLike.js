//
//  MediaManagerApi/lib/NotificationsWsLike.js: Exports a WebSocket like interface
//    in order to receive notifications as defined here:
//
//    http://projects.jetsonsys.com/projects/plm-media-manager-web-api/wiki/NotificationsApi
//
//    The exported interface is intended to be compatible with the HTML WebSocket interface.
//    See:
//
//    http://dev.w3.org/html5/websockets/#the-websocket-interface
//    http://www.websocket.org/aboutwebsocket.html
//
//    Specifically, the following API is exported:
//
//      Constructor: NotificationsWsLike(url, protocol)
//        * Note, protocol is ignored.
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
//      var ws = new WebSocket('ws://appjs/notifications');
//
//      Notes:
//        * the protocol must be 'ws' or 'wss'.
//        * the host MUST be 'appjs'.
//

var _ = require('underscore');
var notifications = require('./Notifications');

//
// WsLikeConstructor: Create a WebSocket like object.
//
//  Notes:
//    * readyState is CONNECTING when object is constructed.
//    * readyState changes to OPEN 0.1 sec latter via a timer to emulate a
//      connection being establish. At that time onopen is invoked.
//
module.exports = function WsLikeConstructor(url, protocol) {

  var that = this;

  this.URL = url;

  this.CONNECTING = 0;
  this.OPEN = 1;
  this.CLOSING = 2;
  this.CLOSED = 3;

  //
  //  Make the state connecting after construction.
  //
  var readyState = this.CONNECTING;
  var bufferedAmount = 0;

  this.onopen = undefined;
  this.onmessage = undefined;
  this.onclose = undefined

  //
  // Emulate the connection opening 0.1 sec. latter by invoking openConnection 
  // and setting readyState appropriately. The timer is the last thing in the
  // constructor.
  //
  
  function openEvent() {
    return {};
  }

  function openConnection() {
    readyState = that.OPEN;
    if (that.onopen) {
      that.onopen(openEvent());
    }
    if (that.onmessage) {
      that.onmessage({ data: JSON.stringify({resource: '/notifications',
                                             event: 'connection.established' })});
    }
  };

  this.send = function(event) {
    var parsedEvent;

    if (readyState !== this.OPEN) {
      //
      //  Silently return if we haven't considered ourselves open yet.
      //
      return;
    }

    try {
      parsedEvent = JSON.parse(event);
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
      sendSubscribe(parsedEvent);
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

  //
  // close: support for emulating a close.
  //
  //  For generating a close event, see:
  //
  //    https://developer.mozilla.org/en-US/docs/WebSockets/WebSockets_reference/CloseEvent
  //
  var closeCodes = {
    NORMAL: 1000
  };
  
  function closeEvent(code) {
    return {
      code: code,
      reason: "UserRequestedClose",
      wasClean: true
    };
  };

  function closeConnection() {
    readyState = that.CLOSED;
    if (that.onclose) {
      that.onclose(closeEvent(closeCodes.NORMAL));
    }
  }

  //
  // close: The public close method.
  //
  this.close = function() {
    setTimeout(closeConnection, 0.1);
  };

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
      console.log('MediaManagerApi/lib/NotificationsWsLike.sendSubscribe: Sending subscripiton event...');
      function handleCallbacks(msg) {
        if (that.onmessage) {
          that.onmessage({ data: JSON.stringify(msg) });
        }
      }
      var subscription = notifications.subscribe(parsedEvent.data.resource, handleCallbacks);
      subscriptions[parsedEvent.data.resource] = subscription;
    }
  };

  //
  // sendUnsubscribe: like sendSubscribe.
  //
  function sendUnsubscribe(parsedEvent) {
    if (_.has(subscriptions, parsedEvent.data.resource)) {
      notifications.unsubscribe(subscriptions[parsedEvent.data.resource]);
      delete subscriptions[parsedEvent.data.resource];
    }
  };

  setTimeout(openConnection, 0.1);

 };
