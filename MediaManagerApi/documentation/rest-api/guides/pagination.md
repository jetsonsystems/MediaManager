# Pagination

Paging through a collection of resources is supported via **cursors**. Endpoints which **index** a resource and hence return a collection may support pagination. One such endpoint is [Importers - index](../endpoints/importers.md#importers-index). See [Endpoints Supporting Pagination](#endpoints-supporting-pagination) for a complete list.

## Making Requests

In order to page threw a collection of resources, a **cursor** must be specified as a parameter to the request. The following is a complete list of paramaters relevant to pagination:

  * **cursor=\<cursor value\>**: Cursor signifying the beginning of the page.
  * **page_size=\<integer\>**: The size of the page to return. Defaults to **10**.
  * **page_to=\<page\>**: To optionally specficy whether the previous or next page should be retrieved:
    * **\<page\>** ::= 'previous' | 'next'

To initiate paging thru a collection, an initial cursor value of -1 should be provided. 

All API responses contain the representation of the resource within an envelope. *Paginated* responses will contain a "paging" attribute. See [Response Payload](#response-payload). 

To continue to the next page, the **cursor** paramter should be set to **start** (the current page), and **page_to** should be set to **next**. Note, the **cursor** parameter could have been set to the **next** cursor value found within the paging section of the envelope, without using **page_to**. But in this case, the **previous** cursor value would NOT be returned in the envelope. Likewise, to go to the previous page, a request should be made where **cursor** is set to that of the current page which is the **start** cursor, and the **page_to** attribute should be set to a value of **previous**.

If it is desirable to know whether a previous page exists, it is recommended that the **page_to** attribute be used to page forward or backward as the **previous** cursor is **ONLY** returned when **page_to** is used.

When the last page of the collection is reached, the **next** cursor value will be **-1**. Likewise, when using **page_to** set to **previous**, the **previous** cursor value will be set to **-1** when the beginning of the collection has been reached.

## Response Payload

All API requests are wrapped in an envelope where meta data related to pagination is contained with the structure referenced by the **paging** attribute. The following is an example envelope for a request to the [Importers - index](../endpoints/importers.md#importers-index) endpoint:

```
  {
    "status": 0,
    "importers": [<importer 1>, … <importer N>],
    "paging": {
      "cursors": {
        "start": <cursor representing the first item in the page>,
        "end": <cursor representing the last item in the page>,
        "previous": <cursor representing the first item in the previous page>,
        "next": <cursor representing the first item in the next page>
      },
      "page_size": <number of pages in the page>
    }
```
The **paging** attribute contains a section of **cursors**:

  * **start**: Cursor corresponding to the first item of the returned page.
  * **end**: Cursor corresponding to the last item of the returned page.
  * **previous**: Cursor corresponding to the first item of the previous page. The **previous** cursor is ONLY provided when the **page_to** parameter was used to retrieve a page.
  * **next**: Cursor corresponding to the first item of the next page.

## Endpoints Supporting Pagination

The following endpoints support pagination:

  * [Importers - index](../endpoints/importers.md#importers-index)

## References

  * Twitter API:
    * [Using cursors to navigate collections | Twitter Developers](https://dev.twitter.com/docs/misc/cursoring)
  * Facebook API:
    * [Pagination - Facebook Developers](https://developers.facebook.com/docs/reference/api/pagination/)

  


