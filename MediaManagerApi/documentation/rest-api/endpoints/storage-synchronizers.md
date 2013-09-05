# Storage/Synchronizers Resource Endpoints

  * [Storage/Synchronizers - create](#storage-sync-create)
  * [Storage/Synchronizers - show](#storage-sync-show)

<a name="storage-sync-create"></a>
## Storage/Synchronizers - create

```
  POST /storage/synchronizers/
```

### Description

Trigger the synchronization of data between the local client and remote storage. The client may also utilize the [[NotificationsApi|Notifications API]] to receive [[NotificationsApi#API-Service-storagesynchronizers-Events|/storage/sychronizers events]] in order to receive notification on the progress of data synchronization.

### Parameters

None

### Request Payload

None or empty, although this is a HTTP POST request.

### Response Payload

The response payload will contain a **synchronizer** attribute which is a JSON representation of the **storage/synchronizers** resource as described here: [Storage/Synchronizer Resource Format](../resource-formats.md#storage-sync-resource-format).

### Examples

  * request:

```
  POST /storage/synchronizers
```
  * response:

```
    {
      "status": 0,
      "synchronizer": {
        "id": "$ee8370c0-be09-445c-b7f4-4cba853150ed+7980b24b-9735-4ef8-a3d1-ecc3f176f8dc",
        "state": "triggered",
        "push": {
          "id": "$ee8370c0-be09-445c-b7f4-4cba853150ed",
          "state": "completed"
        },
        "pull": {
          "id": "$7980b24b-9735-4ef8-a3d1-ecc3f176f8dc",
          "state": "triggered"
        }
      }
```

<a name="storage-sync-show"></a>
### Storage/Synchronizers - show

```
  GET /storage/synchronizers/<synchronizer ID>
```

### Description

Show the state of a current or past synchronization.

The client may also utilize the [PLM Media Manager - Notifications API](../../notifications-api/README.md) to receive [/storage/sychronizers events](../../notifications-api/README.md#API-Service-storagesynchronizers-Events) in order to receive notification on the progress of data synchronization.

### Parameters

None

### Request Payload

None

### Response Payload

The response payload will contain a **synchronizer** attribute which is a JSON representation of the **storage/synchronizers** resource as described here: [Storage/Synchronizer Resource Format](../resource-formats.md#storage-sync-resource-format).

### Examples

  * request:

```
  GET /storage/synchronizers/<synchronizer ID>
```
  * response:

```
    {
      "status": 0,
      "synchronizer": {
        "id": "$ee8370c0-be09-445c-b7f4-4cba853150ed+7980b24b-9735-4ef8-a3d1-ecc3f176f8dc",
        "state": "completed",
        "push": {
          "id": "$ee8370c0-be09-445c-b7f4-4cba853150ed",
          "state": "completed"
        },
        "pull": {
          "id": "$7980b24b-9735-4ef8-a3d1-ecc3f176f8dc",
          "state": "completed"
        }
      }
    }
```