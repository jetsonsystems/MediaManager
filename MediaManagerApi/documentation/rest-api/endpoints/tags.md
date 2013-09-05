# Tags resources endpoints

A Tag is not a full-fledged entity on its own, but is instead a simple string that may be assigned to various types of entities.  There are operations that will involve the management of tags. The endpoints which provide this functionality are the following:

  * [Tags - index](#tags-index)
  * [Tags - show distinct in image collection](#tags-show-images)

<a name="tags-index"></a>
## Tags - index

```
  GET /tags    (completed)
```

### Description

Return a distinct list of all existing tags, sorted in alpha ascending order.

<a name="tags-show-images"></a>
## Tags - show distinct in image collection

```
  GET /tags?images=$imgId1,$imgId2... (completed)
```

### Description

Return the list of distinct tags for a list of Images.
Eventually this could be extended to other lists of images, such as an Album.