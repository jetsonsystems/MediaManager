# Apps/History Resource Endpoints

Apps/History Resources represent request history of a specific type in the context of a specific PLM application. No **Apps/History - update** method is provided as history can ONLY be **indexed**, **created**, and **showed**. Deletion is also not supported currently. The following endpoints are provided:

  * [Apps/History - create](#apps-history-create)
  * [Apps/History - index](#apps-history-index)
  * [Apps/History - show](#apps-history-show)

<a name="apps-history-create"></a>
## Apps/History - create

```
    POST /apps/<app name>/history/<history type>
        <app name> ::= 'photo-manager'
        <history type> ::= 'searches'
```

### Description

Adds a new most recent item to the history of the specified type.

### Resource Representation in Request

It is sufficient to only provide the **href** attribute of the representation in [Apps/History Resource Format](#apps-history-resource-format). If the **href** attribute is provided, any other provided attributes are overwriten by parsing the URL found in the *href* attribute. If the *href* attribute is **NOT** provided it is up to the client to provide a complete representation of the history item in order for it to be useful.

### Example

  * request:

```
    POST /apps/photo-manager/history/searches/

    {
      "href": "http://localhost:9001/api/media-manager/v0/images?tags=weekend,park
    }
```
  * response:

```
    {
      "status": 0,
      "search": {
        "href": "http://localhost:9001/api/media-manager/v0/images?tags=weekend,park
        "protocal": "http",
        "host": "localhost:9001",
        "port": 9001,
        "pathname": "/api/media-manager/v0/images",
        "search": "?tags=weekend,park",
        "query": "tags=weekend,park"
      }
    }
```

<a name="apps-history-index"></a>
## Apps/History - index

```
    GET /apps/<app name>/history/<history type>
        <app name> ::= 'photo-manager'
        <history type> ::= 'searches'
```

### Description

Indexes history of the specified type.

### Example

  * request:

```
    GET /apps/photo-manager/history/searches
```
  * response:

```
    {
      "status": 0,
      "searches": [
        {
          "href": "http://localhost:9001/api/media-manager/v0/images?tags=weekend,park
          "protocal": "http",
          "host": "localhost:9001",
          "port": 9001,
          "pathname": "/api/media-manager/v0/images",
          "search": "?tags=weekend,park",
          "query": "tags=weekend,park"
        },
        {
          "href": "http://localhost:9001/api/media-manager/v0/images?tags=beach,sunset
          "protocal": "http",
          "host": "localhost:9001",
          "port": 9001,
          "pathname": "/api/media-manager/v0/images",
          "search": "?tags=beach,sunset",
          "query": "tags=beach,sunset"
        }
      ]
    }
```


<a name="apps-history-show"></a>
## Apps/History - show

```
    GET /apps/<app name>/history/<history type>/<item index>
        <app name> ::= 'photo-manager'
        <history type> ::= 'searches'
```

### Description

Retrieves a specific history item. <item index> is offset from 0. An \<item index\> of 0, retrieves the most recent history item. The response body will contain a field equivalent to the singular form of the \<history type\>. For example, in the case of a \<history type\> of 'searches', a 'search' field will be present. Each history resource is represented in its [Apps/History Resource Format](#apps-history-resource-format). The attributes of the object are as describe in [Node.js's Parsed URL objects](http://nodejs.org/api/url.html).

### Example

  * request:

```
    GET /apps/photo-manager/history/searches/0
```
  * response:

```
    {
      "status": 0,
      "search": {
        "href": "http://localhost:9001/api/media-manager/v0/images?tags=weekend,park
        "protocal": "http",
        "host": "localhost:9001",
        "port": 9001,
        "pathname": "/api/media-manager/v0/images",
        "search": "?tags=weekend,park",
        "query": "tags=weekend,park"
      }
    }
```