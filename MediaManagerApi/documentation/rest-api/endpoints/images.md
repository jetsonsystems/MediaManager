# Image Resource Endpoints

## <a id="images-index"></a>Images - index

```
    GET /images
```

h5. Description

Returns a list of image resources. Each image resource is represented in its [[RestApi#Image-Resource-Format-Short-Form|Image Resource Format - Short Form]].

h5. Parameters

  * *trashState=out|any|in*: Indicates whether to return (i) images out of trash, or (ii) all regardless of trash state, or (iii) in trash, respectively.  Defaults to trashState=out when parameter is omitted.  In other words, by default hide images that have been placed in trash.
  * filtering images by *tags*:

    * *tags=[<list of tags>]*: Return images which only contain these tags subject to tag_query_op which is optional. Specifying tags= with NO parameter value implies that ONLY images with NO tags assigned are to be return.

    * *tag_query_op=AND|OR*: The parameter qualifies how tags= is to behave. That is:

      * tag_query_op=OR: given a list of tags, show images that contain at least one tag in the list ('or' search). For example: <pre>GET /images?created=2012&tags=tag1,tag2,tag3...</pre>.
      * tag_query_op=AND: given a list of tags, show images that contain all tags in the list ('and' search). For example: <pre>GET /images?created=2012&tags=tag1,tag2,tag3 ...&tag_query_op=AND</pre>.

    * Note, specifying tags= without a value for tag_query_op=, the query defaults to 'tag_query_op=OR', and it would be valid to pass that parameter explicitly as well.


h5. Parameters Planned, but not Implemented

  * imported: Filter returned images based upon their inclusion in a batch of images which were imported into PLM. The parameter is defined as follows: <pre>
    imported=<import selector>

    <import selector> ::= 'latest' | <date selector>
</pre>. A value of *latest* selects the most recent import. A <date selector> will select images from any imports where the completion date of the import is selected by the <date selector>. See See [[RestApi#Timestamps-Dates-and-Date-Selectors-in-Queries|Timestamps, Dates and Date Selectors in Queries]] for details on valid <date selectors>.
  * created: Filter returned images based upon when they were created (imported into PLM). See [[RestApi#Timestamps-Dates-and-Date-Selectors-in-Queries|Timestamps, Dates and Date Selectors in Queries]].
  * taken: Filter returned images based upon when they were taken. See [[RestApi#Timestamps-Dates-and-Date-Selectors-in-Queries|Timestamps, Dates and Date Selectors in Queries]].

Note, the *imported*, *created*, and *taken* parameters need not be mutually exclusive. They can be used in combination to filter the set of images returned. The filters are applied in the following order: *imported* if present, *created* if present, and followed by *taken* if present. If no filtering parameters are applied, then all images are returned.

h5. Examples

  * request: <pre>

    GET /images?created=1wk
</pre>
  * response: <pre>

    {
      "status": 0,
      "images": []
    }

</pre>

h3. Images - show

<pre>
    GET /images/<image ID> (Completed)
</pre>

h5. Description

Returns a single image as identified by its <image ID>. The image resource is represented in its [[RestApi#Image-Resource-Format-Full-Form|Image Resource Format - Full Form]].

h5. Examples

<pre>
    GET /images/$2aoelr2c3p90oae8ifao9e8xsnthdoeu4
</pre>

h3. Images - update

<pre>
  PUT /images/<image ID>?<writable attribute paramaters>
</pre>

h5. Description

Updates writable attributes associated with an image resource. 

For example, the in_trash flag on an image is either set to true to send the image to Trash, or set to false to restore the image from Trash. An image sent to Trash can be recovered until either (i) the Trash is emptied, or (ii) the image is explicitly deleted.

h5. Parameters

  * in_trash: Boolean, which denotes whether the image is in trash or not.

h5. Examples

Image goes to trash:
<pre>
PUT /images/$46247ce7-eef1-4ec6-98b4-eb1ed75e5752?in_trash=true
</pre>

Image is recovered from trash:
<pre>
PUT /images/$46247ce7-eef1-4ec6-98b4-eb1ed75e5752?in_trash=false
</pre>

An empty payload is sent.

h3. Images - delete

<pre>
  DELETE /images/<image ID> (completed)
</pre>

h5. Description

Performs a hard-delete of an image, meaning that it will be deleted from storage and may not be recoverable.  Note that an image is deleted irrespective of whether or not it is in the Trash.  Make sure that you know what you are doing before invoking this method.

h3. Images - empty trash / delete all

<pre>
  DELETE /images[?trashState=in|out|any] (completed)
</pre>


h5. Description

Invoking the images endpoint via an HTTP delete will, by default, permanently delete all the images that are in the Trash (anImage.in_trash = true).  Optionally, the *trashState=any* parameter may be passed to indicate that all images should be deleted regardless of whether they are "in trash" or not.

h5. Parameter

*trashState=in|any* : Optional parameter to filter delete by the value of the image.in_trash attribute. If omitted, defaults to *in*.   
* *in or parameter omitted* : only images in Trash are deleted permanently
* *any* : delete all images (clear storage of all images).  Make sure that you know what you are doing when using this parameter.