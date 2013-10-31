//
// ChildProcessManager: Manages child processes which can be registered / unregister
//  in /var/run.
//
var path = require('path');
var fs = require('fs');
var log4js = require('log4js');

var _ = require('underscore');

var osxFs = require('./OSXFileSystem');
var runDir = osxFs.libAppSupportRunDir;

var moduleName = './lib/ChildProcessManager';

var logPrefix = moduleName + ': ';

var logger = log4js.getLogger('plm.MediaManagerAppSupport');

if (!(fs.existsSync(runDir) && fs.statSync(runDir).isDirectory())) {
  throw {
    name: 'RunDirDoesNotExist',
    message: logPrefix + 'Invalid run directory - ' + runDir
  };
}

//
//  pidFile: Return a path to the PID file.
//
function pidFile(name) {
  return path.join(runDir, name + '.pid');
}

//
// register: Registers a child process given a 'name' and 'pid'. 
//  A pid file is created: name.pid whcih contains the processes PID.
//  
//
module.exports.register = function(name, pid) {
  var lp = logPrefix.replace(': ', '.register: ');

  logger.info(lp + 'Registering process name - ' + name + ', pid - ' + pid);
  try {
    var pidFilePath = pidFile(name);
    fs.writeFileSync(pidFilePath, pid);
  }
  catch (e) {
    throw e;
  }
};

//
// unregister: Unregister the process associated with name.
//  The process will be killed (unless options.kill === false is passed in),
//  and the PID file removed.
//
//  Args:
//    name: Name to identify the process to unregister. If not provided, ALL
//      processes will be unregistered.
//    options:
//      kill: Whether to kill the process. Default is true.
//  
module.exports.unregister = function(name, options) {

  name = _.isString(name) ? name : undefined;
  options = options || (_.isObject(name) ? name : {});

  var lp = logPrefix.replace(': ', '.unregister: ');

  function unregisterPidFile(pidFile) {
    if (fs.existsSync(pidFile)) {
      if (options.kill !== false) {
        var data = undefined;
        try {
          data = fs.readFileSync(pidFile);
        }
        catch (err) {
          data = undefined;
        }
        if (data) {
          var pid = parseInt(data.toString());
          logger.info(lp + 'Attempting to kill process w/ pid - ' + pid);
          
          if (pid !== undefined) {
            try {
              process.kill(pid);
              logger.info(lp + 'Killed process w/ pid - ' + pid + ', pid file - ' + pidFile);
            }
            catch (err) {
              logger.error(lp + 'Could not kill process w/ pid - ' + pid + ', pid file - ' + pidFile);
            }
          }
        }
      }
      fs.unlink(pidFile);
      logger.info(lp + 'PID file unlinked - ' + pidFile);
    }
    else {
      logger.info(lp + 'File does NOT exist, skipping - ' + pidFile);
    }
  }

  if (name) {
    unregisterPidFile(pidFile(name));
  }
  else {
    _.map(fs.readdirSync(runDir).filter(function(pidFile) {
      return pidFile.search(/\.pid$/) !== null;
    }),
          function(pidFile) {
            unregisterPidFile(path.join(runDir, pidFile));
          });
  }
};
