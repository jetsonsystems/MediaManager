# Importers Resource Endpoints

  * [Importers - create](#importers-create)
  * [Importers - index](#importers-index)
  * [Importers - show](#importers-show)
  * [Importers - update](#importers-update)
  * [Importers/images - index](#importers-images-index)

<a name="importers-create"></a>
## Importers - create

```
    POST /importers
```

### Description

Creates a new image *importer* which triggers import of files in the PLM Media Manager from a directory on the user's filesystem. 

### Parameters

  * **dive=\<boolean\>**: Defaults to *true* and causes the import process to explore all sub-directories of import_dir. If *false* is specified, then only files within *import_dir* are considered.

### Request Payload

```
  {
    import_dir: <path on local filesystem to import files from>
  }
```

**import_dir** defines the directory to import from, and possibly sub-directories. The attribute is required.

### Response Payload

The response payload will contain a *importer* attribute which is a JSON representation of *importer* resource as described here: [[RestApi#Importer-Resource-Format|Importer Resource Format]].

### Examples

  * request:

```
    POST /importers?dive=false
```
  * response:

```
    {
      "status": 0,
      "importer": {
        "id": "$ff01ssdw",
        "import_dir": "/Users/marekjulian/really-awesome/photos/",
        "started_at": "2012-12-10T12:00:01Z",
        "completed_at": "2012-12-10T12:00:06Z",
        "num_to_import": 82,
        "num_imported": 0,
        "num_success": 0,
        "num_error": 0
      }
    }
```

<a name="importers-index"></a>
## Importers - index

```
    GET /importers/
```

### Description

Index the last N imports. 

### Parameters

  * **n=\<integer\>**: The number of imports to return. If n is NOT provided, **all** importers are returned.
  * **filter_no_images=\<boolean\>**: Filter importers such that those with no associated images are no returned. **Default: true**
  * **filter_all_in_trash=\<boolean\>**: Filter importers such that those where all images are in trash are not returned. **Default: true**
  * **filter_not_started=\<boolean\>**: Filter importers such that those which have not yet started their import process are not returned. **Default: true**.

Note, by default, when the request is simply <pre>GET /importers</pre>:
  * All importers are returned, with the exception of those filtered by the various filter* options.
  * Importers with no images are NOT returned.
  * Importers where all images are in trash are NOT returned.
  * Importers which have not yet started the import process are NOT returned.

### Response Payload

The response payload will contain a **importers** attribute which is an array of JSON representations of **importer** resources as described here: [Importer Resource Format](../resource-formats.md#importer-resource-format).

### Examples

  * request:

```
    GET /importers?n=2
```
  * response:

```
    {
      "status": 0,
      "importers": [
        {
          "id": "$ff01aabb",
          "import_dir": "/Users/marekjulian/really-awesome-photos/",
          "started_at": "2012-12-10T12:00:01Z",
          "completed_at": "2012-12-10T12:00:06Z",
          "num_to_import": 82,
          "num_imported": 0,
          "num_success": 0,
          "num_error": 0
        },
        {
          "id": "$ff012233",
          "import_dir": "/Users/marekjulian/more-really-awesome-photos/",
          "started_at": "2012-13-10T12:00:01Z",
          "completed_at": "2012-13-10T12:00:06Z",
          "num_to_import": 92,
          "num_imported": 92,
          "num_success": 90,
          "num_error": 2
        }
      ]
    }
```

<a name="importers-show"></a>
## Importers - show

```
    GET /importers/<importer ID>
```

### Description

Gets the current state of an *importer*. A representation of an *importer* can be used to determine the status of an import. In other words, a client can repeatedly poll this endpoint to monitor the progress of an initiated import, the status of a completed import.

### Parameters

None

### Response Payload

The response payload will contain a **importer** attribute which is a JSON representation of **importer** resource as described here: [Importer Resource Format](../resource-formats.md#importer-resource-format).

### Examples

  * request:

```
    GET /importers/$ff01aabb
```
  * response:

```
    {
      "status": 0,
      "importer": {
        "id": "$ff01aabb",
        "import_dir": "/Users/marekjulian/really-awesome-photos/",
        "started_at": "2012-12-10T12:00:01Z",
        "completed_at": "2012-12-10T12:00:06Z",
        "num_to_import": 82,
        "num_imported": 0,
        "num_success": 0,
        "num_error": 0
      }
    }
```

<a name="importers-update"></a>
## Importers - update

```
  PUT /importers/<importer ID>
```

### Description

Updates writable attributes associated with an importer resource. 

Currently, **ONLY** the **state** attribute can be updated. See [Importer Resource Format](../resource-formats.md#importer-resource-format).

### Parameters

None.

### Examples

Import is aborted.
```
PUT /importers/$46247ce7-eef1-4ec6-98b4-eb1ed75e5752

{
  "state": "abort-requested"
}
```

<a name="importers-images-index"></a>
## Importers/images - index

```
  GET /importers/<importer ID>/images
```

### Description

Indexes the images imported via an instance of an **importer**.

### Parameters

None.

### Response Payload

The response payload will contain a **importer** attribute which is a JSON representation of the **importer** resource as described here: [Importer Resource Format](../resource-formats.md#importer-resource-format). In addition, the **importer** attribute, will contain an **images** attribute which is an array of JSON representations of **image** resources which were imported by the **importer**. Each image resource is represented in its [Image Resource Format - Short Form](../resource-formats#image-resource-format-short-form).

### Examples

  * request:

```
    GET /importers/$ff01aabb/images
```
  * response:

```
    {
      "status": 0,
      "importer": {
        "id": "$ff01aabb",
        "import_dir": "/Users/marekjulian/really-awesome-photos/",
        "started_at": "2012-12-10T12:00:01Z",
        "completed_at": "2012-12-10T12:00:06Z",
        "num_to_import": 82,
        "num_imported": 2,
        "num_success": 2,
        "num_error": 0,
        "images" : [
          { <an image in short format> },
          { <an image in short format> }
        ]
      }
    }
```

  * **trashState=out|any|in**: 
  indicates whether to return (i) images out of trash, or (ii) all regardless of trash state, or (iii) in trash, respectively.  Defaults to trashState=out when parameter is omitted.  In other words, by default hide images that have been placed in trash.

```
  GET /importers/<importer ID>/images[?trashState=in](Completed)
  GET /importers/<importer ID>/images[?trashState=out](Completed)
  GET /importers/<importer ID>/images[?trashState=any](Completed)
```