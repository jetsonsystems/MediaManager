# API Resource Formats

This section describes the format (structure and attributes) of all resources which can be accessed via API Endpoints. A subset of a resource's attributes may be writable (can be modified via the API). When a resource has writable attributes a **resource - update** method will be provided where those attributes may be passed along with their new values as URL parameters. In the following format descriptions, any writable attributes denoted with **(w)** following the attribute name.

## Contents

  * [Common Resource Format Elements](#common-resource-format-elements)
  * [Image Resource Format - Full Form](#image-resource-format-full-form)
  * [Image Resource Format - Short Form](#image-resource-format-short-form)
  * [Importer Resource Format](#importer-resource-format)
  * [Storage/Synchronizer Resource Format](#storage-sync-resource-format)
  * [User Resource Format](#user-resource-format)
  * [User/Linked-Accounts Resource Format](#user-accounts-resource-format)
  * [App Settings Resource Format](#app-settings-resource-format)
  * [Apps/History Resource Format](#apps-history-resource-format)

## Common Resource Format Elements

The following elements are common to most resource formats:

  * id: A globally unique identifier, which should be considered an opaque string.
  * name: An object may have a name which is not necessarily globally unique. It may be an \<assigned name\> or a \<generated name\> whose format will meet the following requirements:

```
    <name> ::= <assigned name> | <generated name>
    <assigned name> ::= <alphanumeric><name character>+
    <generated name> ::= $<name character>+
    <name character> ::= <alphanumeric> | '_' | '-' | '.'
```
. It is important to note, that \<assigned names\> **MUST** begin with an \<alphanumeric\> and \<generated names\> begin with a '$'.
  * path: Path used to import the object. This path will be relative to any directory used to import the asset.
  * import_root_dir: Root import directory which was used at the time the asset was imported.
  * created_at: Timestamp of when an object was created (or imported into PLM). See [Dates and Timestamps as Resource Attribute Values](./api-characteristics.md#dates-timestamps-attribute-values).

<a name="image-resource-format-full-form"></a>
## Image Resource Format - Full Form

An [Image Resource](./endpoints/images.md) may be represented in a **full** form which contains all available attributes of the resource. The following attributes are available:

  * id: A globally unique identifier, which should be considered an opaque string.
  * name: Name of the image. It can be assigned or automatically generated. It must satisfy the following requirements:

```
    <image name> ::= <assigned name> | <generated name>
    <generated name> ::= <original asset gen. name> | <derived asset gen. name>
    <original asset gen. name> ::= $<basename of image path> (*1)
    <derived asset gen. name> ::= $derived-<width>-<length>.<format> (*1)
```
.
  * path: See [Common Resource Format Elements](#common-resource-format-elements).
  * import_root_dir: See [Common Resource Format Elements](#common-resource-format-elements).
  * importer_id: The **id** of the [Importers Resource](./endpoints/importers.md) used to import the image.
  * disposition: The disposition of the asset: 

```
        <asset disposition: ::= 'original' | 'variant' 
```
. An original asset represents an asset which has been imported into the system. It can have zero or more variants which were derived from it, and stored as distinct but related entities. Variants are represented in the variants attribute.
  * url: A URL to the image.
  * format: Image format.
  * geometry: Array of width / height.
  * size: Image size in pixels.
  * depth: **What is this?**
  * filesize: Size in bytes.
  * checksum: md5 of image file content.
  * taken_at: Timestamp of when the image was taken. See [Dates and Timestamps as Resource Attribute Values](./api-characteristics.md#dates-timestamps-attribute-values).
  * created_at: Timestamp of when the image was created (or imported into PLM). See [Dates and Timestamps as Resource Attribute Values](./api-characteristics.md#dates-timestamps-attribute-values).
  * variants: A list of variants of the original which are available. Each image variant is represent using the [Image Resource Format - Short Form](#image-resource-format-short-form) representation.
  * in_trash **(w)**: true if the image is in trash.

<a name="image-resource-format-short-form"></a>
## Image Resource Format - Short Form

An [Image Resource](./endpoints/images.md) may be represented in a **short** form which exposes fewer attributes. The [Image Resource Format - Short Form](#image-resource-format-short-form) representation is typically used when an API returns a collections of [Image Resources](./endpoints/images.md), such as [Images - index](./endpoints/images.md#images-index). The following attributes are exposed in the [Short Form](#image-resource-format-short-form) representation:

  * id: A globally unique identifier, which should be considered an opaque string.
  * name: See [Image Resource Format - Full Form](#image-resource-format-full-form).
  * importer_id: The **id** of the [Importers Resource](./endpoints/importers.md) used to import the image.
  * url: See [Image Resource Format - Full Form](#image-resource-format-full-form).
  * geometry: Array of width / height.
  * size: Image size in pixels.
  * filesize: Size in bytes.
  * taken_at: Timestamp of when the image was taken. See [Dates and Timestamps as Resource Attribute Values](./api-characteristics.md#dates-timestamps-attribute-values).
  * created_at: Timestamp image was created (or imported into PLM). See [Dates and Timestamps as Resource Attribute Values](./api-characteristics.md#dates-timestamps-attribute-values).
  * variants: A list of variants of the original which are available. Each image variant is represent using the [Image Resource Format - Short Form](#image-resource-format-short-form) representation.
  * in_trash **(w)**: true if the image is in trash.

<a name="importer-resource-format"></a>
## Importer Resource Format

  * id: ID to reference the importer in subsequent polling.
  * import_dir: The directory used to import objects from.
  * state **(w)**: State of the import process.

```
    state: <import state>
    <import state> ::= 'init' | 'started' | 'abort-requested' | 'aborting' | 'aborted' | 'completed'
```
. Note, the attribute is writable, but it is only permissible to update the value from 'started' to 'abort-requested'.
  * started_at: Time at which the import was initiated.
  * completed_at: A timestamp, or undefined if the import is still in progress.
  * num_to_import: Number of images to import.
  * num_imported: Number imported at the time the request was made.
  * num_success: Number of successful imports.
  * num_error: Number of failed imports.

<a name="storage-sync-resource-format"></a>
## Storage/Synchronizer Resource Format

  * id: ID to reference the storage/synchronizer resource.
  * state: Overall state of the synchronization process.

```
    state: <sync state>
      <sync state> ::= 'triggered' | 'completed' | 'error'
```
  * push: Represents the **push** replication associated with the synchronizer.

    * id: ID associated with the **push** replication.
    * state: The state of the **push** replication. See \<sync state\> above.
  * pull: Represents the **pull** replication associated with the synchronizer.

    * id: ID associated with the **pull** replication.
    * state: The state of the **pull** replication. See \<sync state\> above.

## User Resource Format

  * profile: User's profile.

    * fullname **(w)**: Users full name, for example: "Marek J. Ryniejski".
    * email **(w)**: Email address.
    * organization **(w)**: Text describing the organization the user is affiliated with.
  * account: User's PLM account information.

    * username: Username used for the purpose of authentication.

<a name="user-accounts-resource-format"></a>
## User/Linked-Accounts Resource Format

## App Settings Resource Format

An [App-Settings Resource](./endpoints/app-settings.md) contains the following attributes:

  * app: Application instance attributes:

    * id: Application ID. Read only.
    * name **(w)**: Name of the application instance. It is user editable.
  * db: Database information. Normally would not be edited.

    * database **(w)**: The name of the database. Defaults to **plm-media-manager**. This can be edited in case a different database instance is desired.

<a name="apps-history-resource-format"></a>
## Apps/History Resource Format

An [Apps/History Resource](./endpoints/apps-history.md) describes a request made to the API in the context of a particular application. For example, the following request would return an Apps/History resource representation which describes the last search performed in the **photo manager** application:

```
  GET /apps/photo-manager/history/searches/0
```
. The attributes contained in the representation of an Apps/History resource are as describe for [Node.js's Parsed URL objects](http://nodejs.org/api/url.html). Given the URL:

```
  http://user:pass@host.com:8080/p/a/t/h?query=string#hash
```
, the following are attributes which may be returned:
  * href: The full URL that was originally parsed. Both the protocol and host are lowercased. For example:


```
    http://user:pass@host.com:8080/p/a/t/h?query=string#hash

```
  * protocol: The request protocol, lowercased. For example:

```
    http:
```
  * host: The full lowercased host portion of the URL, including port information. For example:

```
    host.com:8080
```
  * auth: The authentication information portion of a URL. For example:

```
    user:pass
```
   * hostname: Just the lowercased hostname portion of the host. For example:

```
    host.com
```
  * port: The port number portion of the host. For example:

```
    8080
```
  * pathname: The path section of the URL, that comes after the host and before the query, including the initial slash if present. For example:

```
    /p/a/t/h
```
  * search: The 'query string' portion of the URL, including the leading question mark. For example:

```
    ?query=string
```
  * path: Concatenation of pathname and search. For example:

```
    /p/a/t/h?query=string
```
  * query: Either the 'params' portion of the query string, or a querystring-parsed object. For example:

```
    query=string or {'query':'string'}
```
  * hash: The 'fragment' portion of the URL including the pound-sign. For example:
  
```
    #hash
```