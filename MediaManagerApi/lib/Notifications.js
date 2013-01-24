'use strict';

//
// MediaManager/MediaManagerApi/lib/Notifications.js
//

var _ = require('underscore');
var uuid  = require('node-uuid');
var postal = require('postal');

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
  var syncPath = '/storage/synchronizers';
  var IMPORTER_PATH = '/importers';

  var resources = {
    "/storage/synchronizers": Object.create(ResourcePrototype,
                                            { resource: { value: syncPath },
                                              channels: { value: [ postal.channel(syncPath, 'sync.#') ] } })
    ,"/importers" : Object.create(ResourcePrototype,
                                            { resource: { value: IMPORTER_PATH },
                                              channels: { value: [ postal.channel(IMPORTER_PATH, 'import.#') ] } })
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

    console.log('MediaManagerApi/lib/Notifications.subscribe: Doing subscription to resource - ' + resource);

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

    console.log('MediaManagerApi/lib/Notifications.subscribe: Subscribed to resource - ' + resource + ' w/ id - ' + subId);

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

console.log('MediaManagerApi/lib/Notifications: Testing uuid - ' + uuid.v4());
