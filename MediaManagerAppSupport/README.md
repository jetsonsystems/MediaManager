# MediaManagerAppSupport

## Overview

MediaManagerAppSupport provides the supporting infrastructure for the [PLM desktop application, PlmApp](https://github.com/jetsonsystems/PlmApp). The folloiwng functionality is support:

  * General application initialization.
  * Initializing the TouchDB storage engine.
  * Launching the [Application Server Web Worker](./lib/AppServWorker.js), which is an independent web worker process. It:
  
    * Starts server instances for the REST API and Notifications APIs.
    * Creates changes feed instance to generate notifications which can be monitored connecting to the Notifications API server endpoint.
    * Creates an instances of a [Media Manager Asset Manager](./lib/AssetManager.js) which also monitors the <b>changes feed</b> and as appropriate triggers uploading of <b>original assets</b>.

[PLM desktop application, PlmApp](https://github.com/jetsonsystems/PlmApp) simply invokes the <b>init</b> passing an instance of <b>AppJS</b> and <b>application routes</b>.

    //
    //  Init the application which implies starting TouchDB.
    //
    var app = require('MediaManagerAppSupport').init(appjs, routes);
    
## API

### MediaManagerAppSupport Module

The module itself is the one and only one entry point which is essentially a factory method returning an instance of an <b>App</b>. Require it as follows:

    requre('MediaManagerAppSupport)(appjs, routes)
    
Arguments to the module are:

  * appjs: An instance of AppJS.
  * routes: Application routes.

### Events

  * localStorageReady: Local storage is ready to use.
  * localStorageInitError: Local storage could not be initialized. The application should be restarted.
  * localStorageShutdown: The local storage (MediaManagerTouchServer) sub-process exited.
  * appServWorkerExit: The application server sub-process exited.

### Classes

#### App

The application instance return by invoking the init() method.

## Sub-Modules

### [AppServWorker](./lib/AppServWorker.js)
An <b>application server</b> which runs as a <b>web worker</b> sub-process. It provides the following functionality:

  * An REST API server
  * A Notifications API server
  * A changes feed instance which POST messages to a message bus in support of the Notifications API server.\
  * An [AssetManager](./lib/AssetManager.js) instance which is responsible for taking care of assets associated with documents captured in our touchdb / couchdb data store.

### [AssetManager](./lib/AssetManager.js)
Monitors an instance of the touchdb / couchdb changes feed for documents which require special attention with respect to their associated assets. For example, when an image document is imported, its original asset is uploaded to Google Drive.

A work queue is maintained, *<b>gdriveUploadQ</b>*, which is a FIFO queue. Items pushed onto the queue contain the following attributes:

  * doc_id: Document ID of the document the asset is associated with. For example, if it is an [plm.Image](./plm-image/README.md) document, it is its *<b>oid</b>* attribute.
  * path: Path to document on local filesystem.
  
An instance of a [gdrive.Uploader](https://github.com/jetsonsystems/MediaManager/blob/master/MediaManagerStorage/lib/gdrive/README.md) is instantiated and passed the *<b>gdriveUploadQ</b>*.


