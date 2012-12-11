//
//  MediaManagerAppSupport: App server side support code. IE:
//
//    - init: To be called on application start. Returns an app instance.
//
//    - app: An instance of our application.
//
//      methods:
//        config: Returns the application configuration.
//
//      Emits the following events:
//        localStorageExit: The local storage process (DB) has exited.
//

var config = require('MediaManagerAppConfig');
var storage = require('./lib/storage.js');

var EventEmitter = require('events').EventEmitter;

var init = function() {
    var app = Object.create(EventEmitter.prototype);

    app.config = function() {
	return config;
    };

    var dataStore = storage.init();

    dataStore.on('localStorageExit', function() {
 	app.emit('localStorageExit');
    });

    return app;
};

module.exports.init = init;
