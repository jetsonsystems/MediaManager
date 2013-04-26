var WebSocketClient = require('websocket').client;

var client = new WebSocketClient();

client.on('connectFailed', function(error) {
  console.log('Connect Error: ' + error.toString());
});

client.on('connect', function(connection) {
  console.log('WebSocket client connected...');

  connection.on('error', function(error) {
    console.log('Connection error: ' + error.toString());
  });

  connection.on('close', function() {
    console.log('Connection closed');
  });

  connection.on('message', function(message) {
    if (message.type === 'utf8') {
      console.log('Received message: ' + message.utf8Data);

      var pMsg = JSON.parse(message.utf8Data);

      if ((pMsg.resource === "/notifications") && (pMsg.event === "connection.established")) {

        console.log('Sending /importers subscription...');
        connection.sendUTF(JSON.stringify({
          "resource": "_client",
          "event": "subscribe",
          "data": {
            "resource": "/importers"
          }}));

        console.log('Sending /storage/synchronizers subscription...');
        connection.sendUTF(JSON.stringify({
          "resource": "_client",
          "event": "subscribe",
          "data": {
            "resource": "/storage/synchronizers"
          }}));

        console.log('Sent subscriptions...');
      }

    }
    else {
      console.log('Received message of invalid type!');
    }
  });

});

client.connect('ws://localhost:9002/notifications');
