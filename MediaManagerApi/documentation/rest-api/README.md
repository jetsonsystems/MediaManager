# REST API
The **PLM Media Manager - REST API** is a web based API which is RESTful in nature. The API provides access to elementary resources, such as images, document importers, storage synchronizers, etc.. In the current initial implementation all request and response formats are JSON. Note, it is anticipated that as the API evolves, not all features can be exposed in a RESTful manner, but where appropriate and possible, access to data is facilitated via endpoints which reference a RESTful resource. The REST API is complimented by the [PLM Media Manager - Notifications API](../notifications-api/README.md) in order to get feedback on operations which may have been triggered by the REST API, and may be completely asynchronously.

## Contents

  * [API Characteristics](./api-characteristics.md)
    * [RESTful Resource URLs](./api-characteristics.md#restful-resource-urls)
      * [Path Prefix](./api-characteristics.md#path-prefix)
      * [Addressing Resources](./api-characteristics.md#addressing-resources)
    * [HTTP Requests](./api-characteristics.md#http-requests)
      * [Common Endpoint Parameters](./api-characteristics.md#common-endpoint-parameters)
      * [Timestamps, Dates and Date Selectors in Queries](./api-characteristics.md#timestamps-dates-queries)
      * [Writable Resource Attributes](./api-characteristics.md#writable-resource-attributes)
    * [HTTP Responses](./api-characteristics.md#http-responses)
      * [HTTP Responses - General Characteristics](./api-characteristics.md#http-responses-gen-characteristics)
      * [Dates and Timestamps as Resource Attribute Values](./api-characteristics.md#dates-timestamps-attribute-values)
  * [API Resource Formats](./resource-formats.md)
    * [Common Resource Format Elements](./resource-formats.md#common-resource-format-elements)
    * [Image Resource Format - Full Form](./resource-formats.md#image-resource-format-full-form)
    * [Image Resource Format - Short Form](./resource-formats.md#image-resource-format-short-form)
    * [Importer Resource Format](./resource-formats.md#importer-resource-format)
    * [Storage/Synchronizer Resource Format](./resource-formats.md#storage-sync-resource-format)
    * [User Resource Format](./resource-formats.md#user-resource-format)
    * [User/Linked-Accounts Resource Format](./resource-formats.md#user-accounts-resource-format)
    * [App Settings Resource Format](./resource-formats.md#app-settings-resource-format)
    * [Apps/History Resource Format](./resource-formats.md#apps-history-resource-format)
  * [API Endpoints](./endpoints/README.md)
    * [Image Resource Endpoints](./endpoints/images.md)
      * [Images - index](./endpoints/images.md#images-index)
      * [Images - show](./endpoints/images.md#images-show)
      * [Images - update](./endpoints/images.md#images-update)
      * [Images - delete](./endpoints/images.md#images-delete)
      * [Images - empty trash / delete all](./endpoints/images.md#images-trash-delete)


