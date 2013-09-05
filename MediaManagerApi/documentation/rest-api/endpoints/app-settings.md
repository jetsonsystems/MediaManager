# App-Settings Resource Endpoints

/app-settings is a singular resource which contains local application settings such as:

  * application ID: Uniquely identifies the application.
  * application name: A user assignable Application Name.
  * database: The name of the database being used by this instance of the Application.

See [App Settings Resource Format](../resource-formats.md#app-settings-resource-format) for details on what a representation of the resource contains.

Note, there is **NO** **App-Settings - create** endpoint as application settings are installed in the application with default values which can be inspected via [App-Settings - show](#app-settings-show) or modified via [[RestApi#App-Settings-update|App-Settings - update]].

The following endpoints are provided:

  * [App-Settings - show](#app-settings-show)
  * [App-Settings - update](#app-settings-update)

<a name="app-settings-show"></a>
## App-Settings - show

```
    GET /app-settings
```

### Description

Returns a representation of the Application Settings resource as described here: [App Settings Resource Format](../resource-formats.md#app-settings-resource-format).

### Examples

  * request:

```
    GET /app-settings
```
  * response:

```
    {
      "status": 0,
      "app-settings": {
        "app": {
          "id": "1468fcaa-2e19-43b2-8f9e-000000000006",
          "name": "PLM-at-scarp-ridge-lodge"
        },
        "db": {
          "database": "plm$chadpike"
        }
      }
    }
```

<a name="app-settings-update"></a>
## App-Settings - update

```
    PUT /app-settings
```

### Description

Updates writable App Settings attributes. The request body should contain a representation of the resource in the payload with writable attributes modified, or a subset of a properly structured resource representation which ONLY includes writable attributes.

### Writable Attributes

Consult [App Settings Resource Format](../resource-formats.md#app-settings-resource-format) for writable attributes which may be modified. For example, they include:

  * app.name: Name of the application.
  * db.database: Name of the database to be used. Data will sync with ALL applications using this database name. 

### database names

Note, the following convention is suggested for database names:

```
  plm$<username>
``` 
Note, '$' is a safe character in the search part of an URL as defined in [rfc1738](http://www.w3.org/Addressing/rfc1738.txt). Furthermore, it is a valid character to be used in naming of [Couchdb Databases](http://wiki.apache.org/couchdb/HTTP_database_API).

Database names should ONLY contain lower case alpha numeric characters and _$()+-. If a \<username\> contains an other character it must be replaced with one of the valid characters (alpha numeric or \_$()+-).

Currently, no mechanism to authenticate a user has been established. So, care must be taken to chose a unique username with in the organization to include in the database name. In future releases, the first time the user installs the application, he will be prompted to either create an account, or signin. The database name will then be assigned based upon the user's username which will be gauranteed to be unique. The implication is that, the user must be online the first time an instance of the application is installed.

### Example

  * Request:

```
    PUT /app-settings

    {
      "app": {
        "name": "PLM-at-scarp-ridge-lodge"
      },
      "db": {
        "database": "plm:chad-pike"
      }
    }
```
  * response:

```
    {
      "status": 0,
      "app-settings": {
        "app": {
          "id": "1468fcaa-2e19-43b2-8f9e-000000000006",
          "name": "PLM-at-scarp-ridge-lodge"
        },
        "db": {
          "database": "plm$chad-pike"
        }
      }
    }
```