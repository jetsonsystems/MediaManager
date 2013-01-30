//
//  MediaManagerConfig/lib/config.js: Our application config. Wraps an instance of
//    a configuration loaded, and provides setters/getters to enforce privacy
//    of properties which should NOT be changed.
//
//    See <app dir>/config/default.js for the structure of the config attributes.
//    A list of modifyable attributes are shown below above each top level attribute
//    declaration.
//
//    To update aspects of the config, do the following:
//
//      config.<attr>( <new app object with a subset of editable objects> )
//
//      IE:
//
//      config.app( { appName: "Marek Jetjson Laptop MediaManager" } )
//
var _ = require('underscore');
var uuid = require('node-uuid');

var _config = require('config');

if (_config.app.appId === undefined) {
  _config.app.appId = uuid.v4();
}

//
//  Set all our immutable sub-elements of the config.
//
if (_.has(_config, "app")) {
  _config.makeImmutable(_config.app, 'appId');
}
if (_.has(_config, "logging")) {
  _.each(_.keys(_config.logging), function(attr) {
    _config.makeImmutable(_config.logging, attr);
  });
}

var config = Object.create({}, { 
  //
  //  App:
  //    Immutable attributes:
  //      appId
  //
  app: {
    set: function(attr) {
      if (_.has(attr, 'appName')) {
        _config.app.appName = attr.appName;
      }
      if (_.has(attr, 'appId') && (_config.app.appId !== attr.appId)) {
        throw "AppId cannot be modified!";
      }
    },
    get: function() {
      return _config.app;
    }
  },
  db: {
    //
    // Muck with this anyway U want.
    //
    value : _config.db,
    writable: true
  },
  logging: {
    //
    // DO NOT mess with this.
    //
    value: _config.logging,
    writable: false
  }
});

module.exports = config;
