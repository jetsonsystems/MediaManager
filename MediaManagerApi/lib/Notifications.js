var uuid  = require('node-uuid');
var postal = require('postal');

var createModule function() {

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
  };

  ResourcePrototype = {
    resource: unknown,
    channel: unknown,
    format: defaultFormatter
  };

  //
  //  Resources: Resources are channels. Instantiate all we need.
  //    Resource (channels) emit events (send messages).
  //    They can be subscribed to, and published to.
  //
  var resources = {
    '/storage/sychronizers': Object.create(ResourcePrototype,
                                           { resource: '/storage/synchronizers',
                                             channel: postal.channel('/storage/synchronizers') })
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
    function callbackWrapper(data, envelope) {
      callback(resources[resource].format(envelope.topic, data));
    };
    var sub = postal.subscribe({ channel: resource ,
                                 callback: callbackWrapper });
    var subId = uuid.v4();
    subscriptions[subId] = sub;
    return subId;
  };

  //
  // unsubscribe: Unsubscribe given a subscription ID.
  //
  function unsubscribe(subId) {
    if (_.has(subscriptions, subId)) {
      subscriptions[subId].unsubscribe();
      delete subscriptions[subId];
    }
  };

  //
  // publish: Resource publishes an event.
  //
  function publish(resource, event, eventData) {
    if (_.has(channels, resource)) {
      var msg = {
        channel: resource,
        topic: event,
        data: eventData
      }
      channels[resource].publish(msg);
    }
  };

  return Object.create({},
                       { subscribe: { value: subscribe },
                         unsubscribe: { value: unsubscribe },
                         publish: { value: publish } } );
};

module.exports = createModule();
