# API Characteristics

## Contents
* [RESTful Resource URLs](#restful-resource-urls)
  * [Path Prefix](#path-prefix)
  * [Addressing Resources](#addressing-resources)
* [HTTP Requests](#http-requests)
  * [Common Endpoint Parameters](#common-endpoint-parameters)
  * [Timestamps, Dates and Date Selectors in Queries](#timestamps-dates-queries)
  * [Writable Resource Attributes](#writable-resource-attributes)
* [HTTP Responses](#http-responses)
  * [HTTP Responses - General Characteristics](#http-responses-gen-characteristics)
  * [Dates and Timestamps as Resource Attribute Values](#dates-timestamps-attribute-values)

## RESTful Resource URLs

### Path Prefix

All URLs will be prefixed with the following **path prefix**: /api/media-manager/<version>, where the version is currently v0. For example, a images resource would be addressed as follows:

<pre>
    http://<hostname>/api/media-manager/v0/images
</pre>

In the remainder of this document, the **path prefix** will be ommitted when describing an API endpoint.

### Addressing Resources

In general, resources should be referred to as described in [RESTful Web Services](http://en.wikipedia.org/wiki/REST#RESTful_web_services), where the HTTP verbs serve the purpose of methods to access a resource as identified by the URL. Whereever possible, the resource and action will be clearly identified using the HTTP method, and path, without relying on URL parameters. URL parameters are primarily  utilized for filtering, perhaps selecting data to include in a response, specifying sorting criteria of results, etc..

An instance of a resource  must be identified via some reference to a resource, ie: /\<resource collection\>/\<reference to resource instance\>. This reference could be:

  * an opaque object identifier
  * a slug
  * a human readable string, such as a username.

This document will utilize the following convention to distinguish between the 3 forms:

  * \<… ID\>, ie: \<user ID\>
  * \<… slug\>, ie: \<contact slug\>
  * \<object name\>, ie: \<username\>

Note, at times it will be difficult to fit all actions required on a resource into the HTTP verbs. For example, the following URLs, may be required:

  * Add an image to a collection: POST /image-collection/<image-collection ID>
  * Share a image collection: PUT /image-collection/share

To prevent (although unlikely) the potential conflict between an action and resource identifier, the following convention is used:

  * METHOD /collection/_\<action\>
  * METHOD /collection/$\<object ID\>/\<sub collection\>/$\<object ID\>…..

The ‘\_’ character is used to prefix an action when it must appear in a path, to disambiguate between an \<object ID\>, \<slug\>, or \<object name\>. ‘$’ will be used to prefix an \<object ID\>. ‘$’ and ‘\_’ are safe characters according to http://www.w3.org/Addressing/URL/url-spec.txt. ‘$’ and ‘\_’ will NOT appear in \<… slugs\>, nor \<object names\>.

Note, when an \<object ID\> is included in a response, it will be returned via an **id** attribute, as described in [API Resource Formats](./resource-formats.md). The **id** attribute will include any required prefix character, such as the '$' character mentioned above. In addition, a valid \<object ID\> may ONLY contain safe characters:

```
    <object ID> ::= $<alphanumeric or safe>+

      <alphanumeric or safe> ::= <alphanumeric> | <safe>
      <safe> ::= $ | - | _ | @ | . | &  | + | - 

```

## HTTP Requests

The current version of the API only supports JSON request payloads, and JSON responses. Hence, requests which require a request body, only accept a content-type of **application/json**. 

[API Endpoints](./endpoints/README.md) are used to access a resource. The remainder of this section discusses general aspects of accessing an [API Endpoint](./endpoints/README.md).

### Common Endpoint Parameters

The following endpoint request parameters are shared by several API end-points. They are:

  * created=\<date selector\>

    * Allows for filtering of objects based upon their creation date, date/time the object was imported into PLM. Note, created should not be confused with the date an image might have originally been taken. See [Timestamps, Dates and Date Selectors in Queries](./api-characteristics.md#timestamps-dates-queries).

<a id="timestamps-dates-queries"></a>
### Timestamps, Dates and Date Selectors in Queries

A query parameter may require a value which is a timestamp, date, or a \<date selector\>. \<date selectors\> are used to select objects based upon an attribute which is a timestamp or date.

Any date value will take the form of YYYY[MM[DD[HH[mm[.SS]]]]] in UTC. A timestamp is a date which includes seconds, ie: YYYYMMDDHHmm.SS.

A \<date selector\> is defined as follows:

```
  <date selector> ::= <date> | <date_range> | 'today' | '1wk' | '1mo' | '3mo' | '6mo' | 'ytd' | '1yr' | '3yr' | '5yr' | '10yr'
```

Parameter values are as follows:

  * date: Select by date. A value of YYYY will select objects with respect to a particular year, YYYYMM will select with respect to a particular month, etc.
  * date_range ::= '['\<date first\>[, \<date last\>]']'

    * Request objects within a date range by providing one or two dates. Providing a single date, defines a time interval which begins at that date.
 
  * today: Selects with respect to today's date.
  * 1wk: Selects with respect to the last 7 calander days. Today is included in the 7 day interval.
  * 1mo, 3mo, 6mo: Request the most recent 30, 90 or 180 days, respectively. Today is included in the interval.
  * ytd: Request the current year since Jan. 1st.
  * 1yr, 3yr, 5yr, 10yr: Request the last 365, 3 * 365, 5 * 365 or 10 * 365 days. Today is included in the interval.

Note, the values of **1wk**, **xmo**, and **xyr** include include the current day, and begin at 00:00 AM of the (x-1)th previous day. For example, if today is 12:00pm on a Sunday, the time interval will begin at 00:00:00AM on the previous Monday.

For example, the **created** parameter may be used to select images which were imported on a particular date, or within a date range. For example, the following might be a valid request to the **images** resource:


```
    GET /images?created=2012
    GET /images?created=20121109
    GET /images?created=created=[201201,201203]
    GET /images?created=today
                         
```

The first form:

```
    GET /images?created=2012
```

selects images imported in the year 2012 UTC. 

The form:

```
    GET /images?created=[201201,201203]
                         
```
selects images imported anytime during the first 3 months of 2012 UTC.

The final form:

```
    GET /images?created=today
```

selects images imported since midnight of the current day.

### Writable Resource Attributes

When attribute(s) of a resource can be modified (writable attributes), a **/resource - update** method will be provided. A representation of the resource should be provided in the request body. The request body can contain ONLY the writable attributes to be modified, or a complete representation of the resource with writable attributes potentially modified.

Note, some API methods currently ONLY support providing modified writable attributes as URL parameters. For example:

```
  PUT /images/<image ID>?in_trash=true
```
The above, sets an image's **in_trash** attribute to true, thereby "moving the image to trash". In future releases all **/resource -update** methods will support providing a representation of the resource in the request body. Those methods explicitly providing support for submitting writable attributes as URL paramaters, will continue to do so as a convenience, and to support backward compatability.

## HTTP Responses

### <a id="http-responses-gen-characteristics"></a>HTTP Responses - General Characteristics

All responses will be of a content-type of **application/json**. HTTP responses convey success or failure of a request via an appropriate HTTP response status code.

In addtion, in the event of a successful request, the JSON response body provides a status field with a value of 0 (this is distinct from the HTTP response status code). For example, the follow JSON could be returned:

```
  { “status” : 0 }
```

If the response was to obtain details of a particular image, for example, the follow could be a valid JSON response body:

```
{
   “status” : 0,
   “image” : { <image resource representation> }
}
```
, where the **image** attribute's value contains a representation of an **image resource**.

In the event of an unsuccessful request, the response will contain the following three fields:

  *  status
  *  error_code
  *  error_message

Other fields may be provided if necessary. The status should be 1 (not zero). The error_code and error_message fields are provided in order to  provide information regarding why the request failed. For example, a failed request might contain a response body like the following:

```
    {
      “status” : 1,
      “error_code” : 101,
      “error_message” : “Photo is private and details are unavailable.”
    }
```

### <a id="dates-timestamps-attribute-values"></a>Dates and Timestamps as Resource Attribute Values

The representation of a resource, may contain attributes which are a date or timestamp. For example, the **created_at** attribute (see [Common Resource Format Elements](./resource-formats.md#common-resource-format-elements)) describes when the object was created (imported into PLM), and is a timestamp.

All dates and/or timestamps in response payloads will be UTC.
    
Dates and timestamps will be ISO8061 format. IE:

```
                http://www.w3.org/TR/NOTE-datetime
                IE: 2012-04-05T18:18:23Z
```
. A date will simply exclude the time element.