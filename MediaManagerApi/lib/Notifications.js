'use strict';

//
// MediaManager/MediaManagerApi/lib/Notifications.js
//

var _ = require('underscore');
var uuid  = require('node-uuid');
var postal = require('postal');
var log4js = require('log4js');

var log = log4js.getLogger("plm.Notifications");

var createModule = function() {

  //
  // defaultFormatter: simply convert to our message format.
  //  Specialize these for different resources if needed.
  //
  function defaultFormatter(event, data) {
    return {
      resource: this.resource,
      event: event,
      data: data
    };
  }

  var ResourcePrototype = {
    resource: undefined,
    channels: [],
    format: defaultFormatter
  };

  //
  //  Resources: Resources are channels. Instantiate all we need.
  //    Resource (channels) emit events (send messages).
  //    They can be subscribed to, and published to.
  //
  var SYNC_PATH = '/storage/synchronizers';
  var IMPORTER_PATH = '/importers';
  var CHANGES_FEED_PATH = '/storage/changes-feed';

  var resources = {
    "/storage/synchronizers": Object.create(ResourcePrototype,
                                            { resource: { value: SYNC_PATH },
                                              channels: { value: [ postal.channel(SYNC_PATH, 'sync.#') ] } }),
    "/importers" : Object.create(ResourcePrototype,
                                 { resource: { value: IMPORTER_PATH },
                                   channels: { value: [ postal.channel(IMPORTER_PATH, 'import.#') ] } }),
    "/storage/changes-feed" : Object.create(ResourcePrototype,
                                            { resource: { value: CHANGES_FEED_PATH },
                                              channels: { value: [ postal.channel(CHANGES_FEED_PATH, 'doc.#') ] } })
  };

  //
  //  Map of subscription IDs to subscriptions.
  //
  var subscriptions = {};

  //
  // subscribe: Subscribe to a resource.
  //
  //  Args:
  //    resource: The resource (channel)
  //    callback: Gets invoked w/
  //      callback(event)
  //
  //  Returns: subId of subscription.
  //
  function subscribe(resource, callback) {
    if (!_.has(resources, resource)) {
      throw Object.create(new Error(),
                          { name: { value: 'SubscribingToInvalidResource' },
                            message: { value: 'MediaManagerApi/lib/Notifications.subscribe: Invalid resource - ' + resource + '!' } });
    }

    log.info('MediaManagerApi/lib/Notifications.subscribe: Doing subscription to resource - %s', resource);

    function callbackWrapper(data, envelope) {
      callback(resources[resource].format(envelope.topic, data));
    }

    var subId = uuid.v4();

    subscriptions[subId] = [];
    _.each(resources[resource].channels,
           function(channel) {
             var sub = channel.subscribe(callbackWrapper);
             subscriptions[subId].append(sub);
           });

    log.info('MediaManagerApi/lib/Notifications.subscribe: Subscribed to resource - %s w/ id - %s', resource, subId);

    return subId;
  }

  //
  // unsubscribe: Unsubscribe given a subscription ID.
  //
  function unsubscribe(subId) {
    if (_.has(subscriptions, subId)) {
      _.each(subscriptions[subId],
             function(subscriptions) {
               _.each(subscriptions, function(subscription) {
                   subscription.unsubscribe();
               });
             });
      delete subscriptions[subId];
    }
  }

  //
  // publish: Resource publishes an event.
  //
  function publish(resource, event, eventData) {
    if (_.has(resources, resource)) {
      var envelope = {
        channel: resource,
        topic: event,
        data: eventData
      };
      postal.publish(envelope);
    }
  }

  return Object.create({},
                       { subscribe: { value: subscribe },
                         unsubscribe: { value: unsubscribe },
                         publish: { value: publish } } );
};

module.exports = createModule();

// console.log('MediaManagerApi/lib/Notifications: Testing uuid - ' + uuid.v4());
