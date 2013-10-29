//
// MediaManagerAppSupport/lib/OSXFileSystem: OSXFileSystem is responsible
//  for ensuring that any directories / files that must reside in the 
//  OSX Library directory exist and are properly initialized. These include:
//
//  * /Users/<username/Library/Application Support/<Application Bundle Identifier>:
//    * ./config: The application will read/write configuration data.
//    * ./var/log: The applicaiton will write logs here.
//    * ./var/data: The application will read/write other application specific data 
//      that needs to be persisted accross invokations and installations of new 
//      versions of the application.
//  * /Users/<username>/Library/Caches/<Application Bundle Identifier>: Cached data, including:
//    * file: Cached files to be accessed on the local filesystem. For example, images stored 
//      as attachments in TouchDB are cached here via the MediaManagerStorage/lib/file-cache module.
//
//  Refer to the MAC Developer Library / OSX Library Directory Details 
//  (http://developer.apple.com/library/mac/documentation//FileManagement/Conceptual/FileSystemProgrammingGuide/MacOSXDirectories/MacOSXDirectories.html#//apple_ref/doc/uid/TP40010672-CH10-SW1) 
//  for specifics on how OSX applications should leverage the Library Directory.
//
//  The OSXFileSystem should be required on application start before MediaManagerAppConfig is 
//  required.
//
//  Notes:
//
//  1. The CFBundleIdentifier found in Info.plist is used to create a PLM application
//     specific Library Directory under Application Support or Chaces.
//  2. Application configuration: 
//    a) OSXFilesystem copies config/default.js from $PLM_APP_BUNDLE_DIR/Resources/config/default.js 
//      (/Application/Plm.app/config/default.js) to 
//      /Users/<username/Library/Application Support/<Application Bundle Identifier>/config/default.js
//      every time the application is launch. This ensures that the defaults are up to date
//      whenever a new version of the application is installed.
//    b) The config/runtime.js is persisted in ~/Library/Application\ Support and hence any 
//      changes are not lost as a result of installing a new version of the applicaiton.
//
//
//  Exported Attributes:
//
//    appBundleDir: The application bundler directory.
//    appBundlerIdentifier: The bundle's application identifier.
//    appBundleConfigDir: Config dir. in the application bundle. Used to get the default config
//      which gets copied to the /Libarary/Application directory (libAppSupportDir).
//
//    libAppSupportDir: Full path to the application support directory, ie:
//
//      /Users/chad/Library/Application Support/com.jetsonsystems.plm
//
//      Sub directories:
//
//        libAppSupportConfigDir: The config directory the APP reads/updates. Persists accross installs.
//        libAppSupportLogDir: Directory where logs are written to. Persists accross installs.
//
//    libCachesDir: Full path to the caches dir, ie:
//
//      /Users/chad/Library/Caches/com.jetsonsystems.plm
//

var fs = require('fs');
var path = require('path');

var _ = require('underscore');
var plist = require('plist');

exports.appBundleDir = undefined;
exports.appBundleIdentifier = undefined;
exports.appBundleConfigDir = undefined;
exports.libAppSupportDir = undefined;
exports.libAppSupportConfigDir = undefined;
exports.libAppSupportLogDir = undefined;
exports.libAppSupportRunDir = undefined;
exports.libAppSupportDataDir = undefined;

if (_.has(process.env, 'PLM_APP_BUNDLE_DIR')) {

  if (!fs.existsSync(process.env.PLM_APP_BUNDLE_DIR)) {
    throw new Error('Application bundle directory ' + process.env.PLM_APP_BUNDLE_DIR + ' as specified by environment variable PLM_APP_BUNDLE_DIR does NOT exist.');
  }

  var bDirStat = fs.statSync(process.env.PLM_APP_BUNDLE_DIR);
  if (!bDirStat.isDirectory()) {
    throw new Error('Application bundle directory ' + process.env.PLM_APP_BUNDLE_DIR + ' as specified by environment variable PLM_APP_BUNDLE_DIR is NOT a directory.');
  }

  exports.appBundleDir = process.env.PLM_APP_BUNDLE_DIR;

  var infoFile = path.join(exports.appBundleDir, 'Contents/Info.plist');

  if (!fs.existsSync(infoFile)) {
    throw new Error('Application bundle does NOT contain info file - ' + infoFile);
  }

  try {
    var info = plist.parseFileSync(infoFile);

    exports.appBundleIdentifier = info.CFBundleIdentifier;
  }
  catch (e) {
    throw new Error('Application info.plist file is invalid, plist.info - ' + infoFile);
  }
}
else {
  throw new Error('PLM_APP_BUNDLE_DIR evironment variable not set. Cannot find Info.plist to determine Application Bundle Identifier.');
}

//
// initLibAppSupportConfig:
//
var initLibAppSupportConfig = function(appSupportDir) {
  var configDir = path.join(appSupportDir, 'config');
  var bundleConfigDir = exports.appBundleConfigDir = path.join(exports.appBundleDir, 'Contents/Resources/config');

  //
  // Make the config dir.
  //
  if (!fs.existsSync(configDir)) {
    try {
      fs.mkdirSync(configDir);
    }
    catch (e) {
      throw new Error('Error creating config directory - ' + configDir);
    }
  }

  //
  // Have to have bundleConfigDir and bundleConfigDir/default.js.
  //
  if (!fs.existsSync(bundleConfigDir)) {
    throw new Error('Invalid application bundle, config directory does NOT exist - ' + bundleConfigDir);
  }

  var bStats = fs.statSync(bundleConfigDir);
  if (!bStats.isDirectory()) {
    throw new Error('Invalid application bundle, config directory is NOT a directory - ' + bundleConfigDir);
  }

  var bundleDefaultFile = path.join(bundleConfigDir, 'default.js');
  if (!fs.existsSync(bundleDefaultFile)) {
    throw new Error('Invalid application bundle, config/default.js does NOT exist - ' + bundleDefaultFile);
  }

  //
  // Update config/default.js from that in the bundleConfigDir.
  //
  var configDefaultPath = path.join(configDir, 'default.js');
  try {
    var cData = fs.readFileSync(bundleDefaultFile);

    fs.writeFileSync(configDefaultPath, cData);
  }
  catch (e) {
    throw new Error('Error updating config defaults in - ' + configDefaultPath);
  }
  return configDir;
};

//
// initLibAppSupportDir: Initialize the Application Support directory.
//
//  Do the following:
//
//    1. If it does NOT exist create it.
//    2. If it does NOT contain a config dir, create it, and copy config/default.js from appBundleDir.
//    3. If the following do NOT exists, create them:
//      var/log
//      var/run
//      var/data
//
var initLibAppSupportDir = function() {

  var dir = path.join(process.env.HOME, '/Library/Application Support/', exports.appBundleIdentifier);

  if (!fs.existsSync(dir)) {
    try {
      fs.mkdirSync(dir);
    }
    catch (e) {
      throw new Error('Error creating Application Support Directory - ' + dir);
    }
  }

  var dirStat = fs.statSync(dir);

  if (!dirStat.isDirectory()) {
    throw new Error('Application Support Directory is NOT a valid directory - ' + dir);
  }

  exports.libAppSupportConfigDir = initLibAppSupportConfig(dir);

  var varDir = path.join(dir, '/var');
  if (!fs.existsSync(varDir)) {
    try {
      fs.mkdirSync(varDir);
    }
    catch (e) {
      throw new Error('Error creating var directory - ' + varDir);
    }
  }

  var logDir = path.join(dir, '/var/log');

  if (!fs.existsSync(logDir)) {
    try {
      fs.mkdirSync(logDir);
    }
    catch (e) {
      throw new Error('Error creating log directory - ' + logDir);
    }
  }
  exports.libAppSupportLogDir = logDir;

  var runDir = path.join(dir, '/var/run');

  if (!fs.existsSync(runDir)) {
    try {
      fs.mkdirSync(runDir);
    }
    catch (e) {
      throw new Error('Error creating run directory - ' + runDir);
    }
  }
  exports.libAppSupportRunDir = runDir;

  var dataDir = path.join(dir, '/var/data');

  if (!fs.existsSync(dataDir)) {
    try {
      fs.mkdirSync(dataDir);
    }
    catch (e) {
      throw new Error('Error creating data directory - ' + dataDir);
    }
  }
  exports.libAppSupportDataDir = dataDir;

  return dir;
};

exports.libAppSupportDir = initLibAppSupportDir();

//
// initLibCachesDir:
//
var initLibCachesDir = function() {
  var dir = path.join(process.env.HOME, '/Library/Caches/' + exports.appBundleIdentifier);

  if (!fs.existsSync(dir)) {
    try {
      fs.mkdirSync(dir);
    }
    catch (e) {
      throw new Error('Error creating Library Caches directory - ' + dir);
    }
  }

  var dStat = fs.statSync(dir);
  if (!dStat.isDirectory()) {
    throw new Error('Library Caches directory is NOT a directory - ' + dir);
  }

  return dir;
};

exports.libCachesDir = initLibCachesDir();
