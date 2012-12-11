//
//  MediaMangerAppSupport: storage initialiation / monitoring.
//
var EventEmitter = require('events').EventEmitter;
var cp = require('child_process');

//
//  init: Initialize storage.
//
//    Args:
//      config - Application configuration.
//      options:
//        replicate - boolean
//
var init = function(config, options) {
    var db = Object.create(EventEmitter.prototype);

    dbChildProcess = cp.spawn('/Users/marekjulian/Library/Developer/Xcode/DerivedData/MediaManagerTouchServ-epksriervtouqsddxpivmdimsxvi/Build/Products/Debug/MediaManagerTouchServer.app/Contents/MacOS/MediaManagerTouchServ', [], {});

    dbChildProcess.on('exit', function(code) {
	db.emit('localStorageExit');
    });
    return db;
};

module.exports.init = init;

