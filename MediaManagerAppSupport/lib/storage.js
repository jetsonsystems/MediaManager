//
//  MediaMangerAppSupport: storage initialiation / monitoring.
//
//    Note, this is mostly process monitoring stuff. Perhaps we can leverage
//    an external module. But, a quick google didn't point to anything obvious.
//
var EventEmitter = require('events').EventEmitter;
var fs = require('fs');
var path = require('path');
var cp = require('child_process');

var runDir = './var/run';

//
//  init: Initialize storage.
//
//    Args:
//      config - Application configuration.
//      options:
//        replicate - boolean
//
//    Returns:
//      storage - An object which is an event emitter. 
//        attributes:
//          execPath: Path used to launch the storage process.
//        events:
//          localStorageExit: When the storage process has exited, and not explicitly shutdown.
//          localStorageShutdown: When the storage process has exited due to a normal shutdown.
//
var init = function(config, options) {

  console.log('MediaManagerAppSupport/lib/storage.init: initializing using config - ' + JSON.stringify(config));

  var database;

  try {
    database = config.db.database;
  }
  catch (err) {
    throw Object.create(new Error(),
                        { name: { value: 'ConfigMissingDBName' },
                          message: { value: 'MediaManagerAppSupport/lib/storage: <config> MUST define a database.' } });
  }

  if (!(fs.existsSync(runDir) && fs.statSync(runDir).isDirectory())) {
    console.log('MediaManagerAppSupport/lib/storage.init: run directory does NOT exist, run dir - ' + runDir);
    var fullPath = path.join(process.cwd(), runDir);
    console.log('MediaManagerAppSupport/lib/storage.init: creating run dir - ' + fullPath);

    try {
      fs.mkdirSync(fullPath);
    }
    catch(err) {
      throw Object.create(new Error(),
                          { name: { value: 'RunDirCreateError' },
                            message: { value: 'MediaManagerAppSupport/lib/storage.init: Error creating run directory - ' + fullPath + ', w/ err - ' + err}
                          });
    }
  }

  var storage = Object.create(EventEmitter.prototype);
  var execPath = config.db.local.execPath;
  storage.execPath = execPath;

  storage.shutdown = function() {
    killChildProcess();
  };

  var childProcess = undefined;
  
  //
  //  createChildProcess: Start a process, and record its PID in the run directory.
  //
  var createChildProcess = function() {
    console.log('MediaManagerAppSupprt.init: Launching storage child process, execPath - ' + execPath + ', database - ' + database);
    var childP = cp.spawn(execPath, ['--db', database], {});
    updatePIDFile(childP.pid);
    return childP;
  };
  
  var didChildProcessKill = false;
  
  //
  //  killChildProcess: Kill the child process, and also remove its PID file from the run directory.
  //    Note, verifies that the PID in the file is that of the child process.
  //
  var killChildProcess = function() {
    if (childProcess !== undefined) {
      console.log('MediaManagerAppSupprt.init.killChildProcess: Attempting to kill child process, pid - ' + childProcess.pid);
      var pidFilePath = pidFile();
      
      var data = undefined;
      try {
        data = fs.readFileSync(pidFilePath);
      }
      catch (err) {
        data = undefined;
      }
      if (data) {
        if (parseInt(data.toString()) === childProcess.pid) {
          childProcess.kill();
          didChildProcessKill = true;
          fs.unlink(pidFilePath);
          console.log('MediaManagerAppSupprt.init.killChildProcess: Killed child process, pid - ' + childProcess.pid);
          childProcess = undefined;
        }
      }
    }
  };
  
  //
  //  cleanRunDir: Check to see the run directory to see if there are ANY
  //    storage processes still around. Similar to killChildProcess,
  //    but we JUST look at a pid file.
  //
  var cleanRunDir = function(execPath) {
    var pidFilePath = pidFile();

    var data = undefined;
    try {
      data = fs.readFileSync(pidFilePath);
    }
    catch (err) {
      data = undefined;
    }
    if (data) {
      var pid = parseInt(data.toString());
      console.log('MediaManagerAppSupprt.init.cleanRunDir: Attempting to kill process w/ pid - ' + pid);

      if (pid !== undefined) {
        try {
          process.kill(pid);
          console.log('MediaManagerAppSupprt.init.cleanRunDir: Killed process w/ pid - ' + pid);
        }
        catch (err) {
          console.log('MediaManagerAppSupprt.init.cleanRunDir: Could not kill process w/ pid - ' + pid);
        }
      }
      fs.unlink(pidFilePath);
    }
  };

  //
  //  updatePIDFile: update the pid file with a numeric PID.
  //
  var updatePIDFile = function(pid) {
    try {
      var pidFilePath = pidFile();
      fs.writeFile(pidFilePath, pid);
    }
    catch (e) {
      throw e;
    }
  };

  //
  //  pidFile: Return a path to the PID file.
  //
  var pidFile = function() {
    var baseName = execPath.split('/').pop();
    if (baseName) {
      return path.join(runDir, baseName + '.pid');
    }
    else {
      throw {
        name: "pidFileUpdateError",
        message: "Could not create PID file from storage exec path of " + execPath
      }
    }
  }

  //
  //  First clean anything that might have NOT been killed so we have a clean run directory.
  //
  cleanRunDir();
  childProcess = createChildProcess();
  
  childProcess.on('exit', function(code, signal) {
    console.log('MediaManagerAppSupport: Child process exit event detected!');
    if ((signal === 'SIGTERM') && (didChildProcessKill === true)) {
      console.log('MediaManagerAppSupport: emitting shutdown event!');
      storage.emit('localStorageShutdown');
    }
    else {
      console.log('MediaManagerAppSupport: emitting exit event!');
      storage.emit('localStorageExit');
    }
    didChildProcessKill = false;
  });
  
  process.on('exit', function() {
    console.log('MediaManagerAppSupport.init: Process exit event detected...');
    if (childProcess) {
      killChildProcess();
    }
  });

  return storage;
};

module.exports.init = init;
