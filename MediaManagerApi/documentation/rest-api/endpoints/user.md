# User Resource Endpoints

A user respource contains a User's profile and account information. It is a singular resource containing the data associated with the user of the application. The following endpoints are available:

  * [User - create](#user-create)
  * [User - show](#user-show)
  * [User - update](#user-update)

<a name="user-create"></a>
## User - create

```
    POST /user
```

### Description

Create a User resource for the user of the application. Writable attributes of the resource in [User Resource Format](../resource-formats.md#user-resource-format) should be provided in the request body.

Note, currently there is NO authentication supported with a central authentication service. Currently, the user resource is captured and stored with the application configuration.

### Payload

The payload should contain a represent of a User resource describing a new user in [[RestApi#User-Resource-Format|User Resource Format]].

### account.username

It is recommended that username's be restricted to alpha numeric characters and the following set of characters which are also valid in a [CouchDB database name](http://wiki.apache.org/couchdb/HTTP_database_API): **_+-** .

### Example:

  * request:

```
    POST /user

    {
      "profile": {
        "fullname": "Marek J. Ryniejski",
        "email": "marek@jetsonsys.com",
        "organization": "Jetson Systems" 
      },
      "account": {
        "username": "marekjulian" 
      }
    }
```
  * response:

```
    {
      "status": 0,
      "user": {
        "profile": {
          "fullname": "Marek J. Ryniejski",
          "email": "marek@jetsonsys.com",
          "organization": "Jetson Systems" 
        },
        "account": {
          "username": "marekjulian" 
        }
      }
    }
```

<a name="user-show"></a>
## User - show

```
    GET /user
```

### Description

Show the User resource for the user of the application. A representation in [[RestApi#User-Resource-Format|User Resource Format]] is returned.

### Examples

  * request:

```
    GET /user
```
  * response:
  
```
    {
      "status": 0,
      "user": {
        "profile": {
          "fullname": "Marek J. Ryniejski",
          "email": "marek@jetsonsys.com",
          "organization": "Jetson Systems"
        },
        "account": {
          "username": "marekjulian"
        }
      }
    }
```

<a name="user-update"></a>
## User - update

```
    PUT /user
```

### Description

Update the User resource for the user of the application. Writable attributes of the resource [User Resource Format](#user-resource-format) should be passed in the payload. A full representation of the resource containing modifications to writable attributes may also be submitted in the request body.

### Writable Attributes

Consult [User Resource Format](#user-resource-format) for writable attributes which may be modified. For example, they include:

  * profile.fullname
  * profile.email
  * profile.organization

### Example

  * request:

```
    PUT /user

    {
      "profile": {
        "fullname": "Marek Julian Ryniejski",
        "email": "marek@jetsonsys.com",
        "organization": "Jetson Systems" 
      }
    }
```
  * response:

```
    {
      "status": 0,
      "user": {
        "profile": {
          "fullname": "Marek Julian Ryniejski",
          "email": "marek@jetsonsys.com",
          "organization": "Jetson Systems" 
        },
        "account": {
          "username": "marekjulian" 
        }
      }
    }
```

Note, a full representation of the resource in returned in the response.