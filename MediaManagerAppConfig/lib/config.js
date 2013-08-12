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
//      config.app( { name: "Marek Jetjson Laptop MediaManager" } )
//
var _ = require('underscore');
var uuid = require('node-uuid');

var _config = require('config');

if (_config.app.id === undefined) {
  _config.app.id = uuid.v4();
}

//
//  Set all our immutable sub-elements of the config.
//
if (_.has(_config, "app")) {
  _config.makeImmutable(_config.app, 'id');
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
  //      id
  //
  app: {
    set: function(attr) {
      if (_.has(attr, 'name')) {
        _config.app.name = attr.name;
      }
      if (_.has(attr, 'id') && (_config.app.id !== attr.id)) {
        throw "App id cannot be modified!";
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
  services: {
    //
    // DO NOT mess with this.
    //
    value: _config.services,
    writable: false
  },
  modules: {
    //
    // DO NOT mess with this.
    //
    value: _config.modules,
    writable: false
  },
  storage: {
    //
    // DO NOT mess with this.
    //
    value: _config.storage,
    writable: false
  },
  linkedAccounts: {
    value: _config.linkedAccounts,
    writable: false
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
