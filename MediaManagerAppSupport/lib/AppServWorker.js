//
// AppServWorker: Webworker which implements 2 services:
//
//  1. REST API
//  2. Notifications Websocket endpoint
//
//  AppServWorker listens on 2 ports specfied in the system configuration for each
//  of the above services.
//
var path = require('path');
var log4js = require('log4js');

var logger = log4js.getLogger('plm.MediaManagerAppSupport');
var tmp = __filename.split('/');
var logPrefix = tmp[tmp.length-1].replace('.js', ': ');

var config = require('MediaManagerAppConfig');
var apiVersion = config.services.restAPI.version;
var pathPrefix = path.join(config.services.restAPI.pathPrefix, apiVersion);

var AppServWorkerMessages = require('MediaManagerAppSupport/lib/AppServWorkerMessages.js');
var mmApi = require('MediaManagerApi/lib/MediaManagerApiCore')(config);

logger.info(logPrefix + 'Starting services...');

var restService = require('MediaManagerAppSupport/lib/AppRestService');

logger.info(logPrefix + 'REST API service started.');

var notifService = require('MediaManagerAppSupport/lib/AppNotifService');

logger.info(logPrefix + 'Notifications API service started.');

//
// Finally, monitor the changes feed.
//
var changesFeed = new mmApi.StorageChangesFeed('/storage/changes-feed',
                                               {instName: 'changes-feed',
                                                pathPrefix: pathPrefix });
var appId = undefined;
var dbInfo = undefined;

logger.info(logPrefix + 'Changes feed created...');

//
// onmessage: Process a message, which should describe the request to handle.
//
//  Currently, there is ONE valid message, should be of type 
//  AppServWorkerMessages.LOCAL_STORAGE_READY. Format of the message is:
//
//    e.data.type: AppServWorkerMessages.LOCAL_STORAGE_READY
//    e.data.appId: Application ID
//    e.data.dbInfo: DB info with fields:
//      dbName
//      docCount
//      updateSeq
//      diskSize
//
//  Upon receipt of:
//    * AppServWorkerMessages.LOCAL_STORAGE_READY: we create the
//    changes feed resource to monitor changes to the DB.
//
onmessage = function(e) {
  var lp = logPrefix.replace(': ', '.onmessage: ');
  if (e.data.type === AppServWorkerMessages.LOCAL_STORAGE_READY) {
    //
    //  Do a create request on the changes feed to start monitoring it.
    //
    appId = e.data.appId;
    dbInfo = e.data.dbInfo;
    logger.info(lp + 'Connecting to changes feed, w/ sequence ID - ' + dbInfo.updateSeq + ', app id - ' + appId);
    changesFeed.create({}, 
                       { query: { since: dbInfo.updateSeq,
                                  exclude_app_id: appId } });
  }
  else {
    logger.info(lp + 'Invalid message received by worker, type - ' + e.data.type);
  }
};

//
// onclose: Make sure everything gets close gracefully.
//
onclose = function() {
  restService.shutdown();
  notifService.shutdown();
};

