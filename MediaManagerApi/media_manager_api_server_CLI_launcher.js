//
//  Media Manager API Server: Implements the media manager APIs which are exposed over HTTP.
//
//    * Current API version is v0.
//    * All request URLs are prefixed with /<version>.
//    * Currently implemented resources are:
//      /images
//    * See the API documentation: http://projects.jetsonsys.com/projects/plm-media-manager-web-api/wiki/Wiki
//      * Note: the /api/media-manager path prefix if required is added by any reverse proxy forwarding requests to this
//        service.
//
//var version = '0.0.1';
//var urlVersion = 'v0';
//var serverName = 'media_manager_api_server';

//var logDir = '/var/log/' + serverName;
//var infoLogfile = serverName + '.log';
//var errorLogfile = serverName + '.log';
var _ = require('underscore');
//var url = require('url');
//var restify = require('restify');

var media_manager_api_server = require('./media_manager_api_server');

var opts = require('optimist')
  .boolean('v')
  .boolean('c')
  .usage('Usage: $0 [<options>] <port>\n\nMedia Manager API Server.')
  .options({
    'c' : {
      'alias' : 'config',
      'default' : false,
      'describe' : 'Use MediaManageAppConfig instead of -d, -h, and/or -p options.'
    },
    'd' : {
      'alias' : 'dbname',
      'default' : 'plm-media-manager',
      'describe' : 'Database name.'
    },
    'h' : {
      'alias' : 'dbhost',
      'default' : 'localhost',
      'describe' : 'TouchDB / CouchDB host.'
    },
    'p' : {
      'alias' : 'dbport',
      'default' : 5984,
      'describe' : 'TouchDB / CouchDB port number.'
    }
  });
var argv = opts.argv;

var argsOk = function(argv) {
  if (argv._.length !== 1) {
    return false;
  }
  return true;
}(argv);

if (!argsOk) {
  opts.showHelp();
  process.exit(1);
}

var serverPort = argv._[0];

var config = undefined;

if (argv.c) {
  config = require('MediaManagerAppConfig');
}
else {
  config = {
    db: {
      database: argv.d,
      local: {
        host: argv.h,
        port: argv.p
        }
      }
    };
}

media_manager_api_server.startServer(serverPort, config, function (err, result) {
    if (err) {
      console.log(err);
    }
    else {
      console.log("Test server started");
    }
  }
);