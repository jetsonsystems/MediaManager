'use strict';

var util = require('util')
  , updateDesignDoc = require('./update_design_doc');


var opts = require('optimist')
  .boolean('v')
  .usage('Usage: $0 [<options>] <dbname>\n\nUpdates design documents in TouchDB / CouchDB. DB must be specified.')
  .options({
    'h':{
      'alias':'host',
      'default':'localhost',
      'describe':'TouchDB / CouchDB host.'
    },
    'p':{
      'alias':'port',
      'default':5984,
      'describe':'TouchDB / CouchDB port number.'
    },
    't':{
      'alias':'type',
      'default':'couchdb',
      'describe':'Specifies db type: couchdb | touchdb, defualt == couchdb'
    }
  });

var argv = opts.argv;

var argsOk = function (argv) {
  if (argv._.length !== 1) {
    console.log('A single <dbname> is required.');
    return false;
  }
  return true;
}(argv);

if (!argsOk) {
  opts.showHelp();
  process.exit(1);
}

var dbName = argv._[0];

var options = {
  host:argv.h,
  port:argv.p,
  dbName:dbName,
  dbType:argv.t
};

if (options.design_doc === 'couchdb') {
  console.log('DB type is couchdb');
}
else {
  console.log('DB type is touchdb');
}


updateDesignDoc.updateDesignDoc(options, function (err, result) {
    if (err) {
      console.log(err);
    }
    else {
      console.log("Updated Design Doc: " + util.inspect(result, true, null, true));
    }
  }

);
