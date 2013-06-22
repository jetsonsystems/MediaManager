//
// MediaManagerAppSupport/lib/AssetManager: Monitors an instance of the 
//  touchdb / couchdb changes feed for documents which require special 
//  attention with respect to their associated assets. For example,
//  when an image document is imported, its original asset is uploaded
//  to Google Drive.
//
//  See README.md for details.
//

var util = require('util');
var _ = require('underscore');
var storageModule = require('MediaManagerStorage');

var log4js = require('log4js');

var moduleName = './lib/AssetManager';

//
// assetManagerModule: Invoking the module will create a changes feed which
//  is monitored, and relevant assets are passed on to the gdriveUploader.
//
//  See ../README.md for further details.
//
module.exports = function assetManagerModule(config, dbUpdateSeq, options) {

  var logger = log4js.getLogger('plm.MediaManagerAppSupport');
  var logPrefix = moduleName + ': ';

  var options = options || {};

  if (!_.has(options, 'dryRun')) {
    options.dryRun = false;
  }

  var appId = config.app.id;

  var storage = storageModule(config.db, {singleton: true});
  var touchdb = storage.get('touchdb');
  var gdrive = storage.get('gdrive');

  //
  // Create a changes feed to monitor, and watch for changes to Image
  // documents.
  //
  var changesFeed = touchdb.changesFeed({since: dbUpdateSeq,
                                         includeFilter: ['plm.Image']});

  var gdriveUploadqFile = config.storage.gdrive.uploadQFile || 'uploadq.json';

  console.log(logPrefix + 'config.storage.gdrive.persistDir - ' + config.storage.gdrive.persistDir);

  var account;

  if (_.has(options, 'dryRun')) {
    account = {};
  }
  else {
    var user = config.storage.gdrive.account.user;
    account = _.find(config.linkedAccounts, function(account) {
      return ((account.type === 'gdrive') && (account.user === user));
    });
  }

  var gdriveUploader = new gdrive.Uploader(account, 
                                           config.storage.gdrive.persistDir,
                                           {dryRun: options.dryRun,
                                            uploadqFile: gdriveUploadqFile,
                                            location: config.storage.gdrive.locations.originals});

  var uploadStats = {
    numChanges: 0,
    numQueued: 0,
    numStarted: 0,
    numSuccess: 0,
    numError: 0,
    numPending: 0,
    queueSize: 0,
    numUploading: 0
  };

  //
  // When we get changes, put pertinent image documents into gdriveUplaodQ.
  //
  changesFeed.on('doc.image.created', 
                 function(event) {
                   uploadStats.numChanges = uploadStats.numChanges + 1;
                   var image = event.doc;
                   // logger.info(logPrefix + 'Testing new image - ' + util.inspect(image));
                   if ((image.orig_id === "") && (_.has(image, 'app_id') && (appId === image.app_id))) {
                     //
                     // Have an Image document corresponding to the original asset.
                     //
                     logger.info(logPrefix + 'Have valid change!');
                     gdriveUploader.enqueue({assetType: "original",
                                             docId: image.oid,
                                             path: image.path});
                     uploadStats.numQueued = uploadStats.numQueued + 1;
                   }
                   else {
                     logger.info(logPrefix + 'Change skipped, original id - ' + image.orig_id + ', image.app_id - ' + image.app_id);
                   }
                 });

  //
  // When successful uploads occur, update the corresponding Image document.
  //
  gdriveUploader.on(gdriveUploader.events.uploadStarted, function(uploadEvent) {
    uploadStats.numStarted = uploadStats.numStarted + 1;
  });
  gdriveUploader.on(gdriveUploader.events.uploadSuccess, function(uploadEvent) {
    uploadStats.numSuccess = uploadStats.numSuccess + 1;
  });
  gdriveUploader.on(gdriveUploader.events.uploadError, function(uploadEvent) {
    uploadStats.numError = uploadStats.numError + 1;
  });

  changesFeed.listen();
  gdriveUploader.start();

  return {
    numPending: function() {
      return gdriveUploader.numPending();
    },

    queueSize: function() {
      return gdriveUploader.queueSize();
    },

    numUploading: function() {
      return gdriveUploader.numUploading();
    },

    stats: function() {
      var uStats = gdriveUploader.stats();
      uploadStats.numPending = uStats.numPending,
      uploadStats.queueSize = uStats.queueSize,
      uploadStats.numUploading = uStats.numUploading;
      return uploadStats;
    }
  };

};
