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

```
USAGE:

  var assetManager = require('MediaManagerAppSupprt/lib/AssetManager')(config,
                                                                       dbUpdateSeq,
                                                                       options)
                                                                       
  
  config:      Application config.
  dbUpdateSeq: TouchDB / CouchDB update sequence to start monitoring the 
               changes feed from.
  options:     
    logOnly:   Run in logging only mode where assets are NOT actually uploaded. 
               When specified, overrides value in config.
               
```

Monitors an instance of the touchdb / couchdb *<b>changes feed</b>* for documents which require special attention with respect to their associated assets. For example, when an image document is imported, its original asset is uploaded to Google Drive.

An instance of a [gdrive.Uploader](https://github.com/jetsonsystems/MediaManager/blob/master/MediaManagerStorage/lib/gdrive/README.md) is maintained. When a new original assets associated with the current application (based upon *<b>app_id</b>* in the application config and the plm.Image document) are received via the *<b>changes feed</b>*, the asset is <b>enqueued</b> in the [gdrive.Uploader](https://github.com/jetsonsystems/MediaManager/blob/master/MediaManagerStorage/lib/gdrive/README.md).

Assets <b>enqueued</b> have  the following attributes:

  * doc_id: Document ID of the document the asset is associated with. For example, if it is an [plm.Image](./plm-image/README.md) document, it is its *<b>oid</b>* attribute.
  * path: Path to document on local filesystem.

#### Application Config
There are several sections of the application config. which are of particular importance, and they are highlighted here.

##### config.db
Defines the database:

```
{ 
  database: "plm-media-manager",
  local: {
    execPath: './MediaManagerTouchServer.app/Contents/MacOS/MediaManagerTouchServ',
    port: "59840",
    updateSeq: undefined
  }
}
```
##### config.storage.gdrive
Configuration for Google Drive Storage:

```
{
    persistDir: "./var/data/storage/gdrive",
    account: {
      type: 'gdrive',
      user: 'marek@jetsonsys.com'
    },
    logOnly: true,
    locations: {
      originals: {
        assetType: "original",
        root: "appdata",
        basePath: "/media/images/originals/",
        folderId: undefined
      }
    }
  }
```

  * logOnly:   Run in logging only mode where assets are NOT actually uploaded. 
               Defauilt: false.


#### Classes
None
#### Methods
None
#### Attributes

  * *<b>uploadStats</b>*: Statistics related to the progress of uploads. Contains the following attributes:
    * numChanges: Number of new documents encountered by monitoring the changes feed.
    * numQueued: Number of original documents created by the current application instance, which were queued for Google Drive upload.
    * numStarted: Number uploads started.
    * numSuccess: Number of upload successes.
    * numError: Number of upload errors.
    
### [OSXFileSystem](./lib/OSXFileSystem.js)
```
USAGE:

  var osxfs = require('MediaManagerAppSupport/lib/OSXFileSystem');
  
```

This module initializes the filesystem used by the application. Specifically, the following directories are initialized:

  * *<b>/Users/\<username\>/Library/Application Support/\<Application Bundle Identifier\></b>*: An instance of the application invoked by a particular user will:
    * read/write configuration data,
    * write logs,
    * read write other application specific data that needs to be persisted accross invokations and installations of new versions of the application.
  * *<b>/Users/\<username\>/Library/Caches/\<Application Bundle Identifier\></b>*: Cached data, including:
    * file: Cached files to be accessed on the local filesystem. For example, images stored as attachments in TouchDB are cached here via the [MediaManagerStorage/lib/file-cache](../MediaManagerStorage/lib/file-cache/README.md) module.

The *<b>Application Bundle Identifier</b>* is obtained by accessing the *<b>CFBundleIdentifier</b>* property of the application bundle's Info.plist file. The location of the Applicaiton Bundle is obtained from the *<b>PLM_APP_BUNDLE_DIR</b>* environment variable. The *<b>PLM_APP_BUNDLER_DIR</b>* must be properly set to point to an application bundle.

Refer to [MAC Developer Library / OSX Library Directory Details](http://developer.apple.com/library/mac/documentation//FileManagement/Conceptual/FileSystemProgrammingGuide/MacOSXDirectories/MacOSXDirectories.html#//apple_ref/doc/uid/TP40010672-CH10-SW1) for specifics on how OSX applications should leverage the *<b>Library Directory</b>*.


#### References

  * [MAC Developer Library / File System Programming Guide](http://developer.apple.com/library/mac/documentation//FileManagement/Conceptual/FileSystemProgrammingGuide/Introduction/Introduction.html)
    * [MAC Developer Library / OSX Library Directory Details](http://developer.apple.com/library/mac/documentation//FileManagement/Conceptual/FileSystemProgrammingGuide/MacOSXDirectories/MacOSXDirectories.html#//apple_ref/doc/uid/TP40010672-CH10-SW1)

