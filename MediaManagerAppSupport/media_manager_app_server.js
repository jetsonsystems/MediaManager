//
// media_manager_app_server: Creates a web worker, which is our app server.
//  This is a harness to conveniently run AppServWorker.js outside of the context of
//  the app itself.
//
var path = require('path');
var log4js = require('log4js');

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

var Worker = require('webworker');

var workerPath = path.join(__dirname, 'lib/AppServWorker.js');
var worker = new Worker(workerPath);

worker.onexit = function(c, s) {
  logger.error(logPrefix + 'AppServWorker exited, code - ' + c + ', signal - ' + s);
};

