# Image Tagger resource endpoints

The Image Tagger API differs from the APIs above in that it does not manage a single entity, or a parent entity with a one-to-many relationship. Instead, it manages a many-to-many association (relationship) between Images and Tags:

* A Tag may be assigned to one or more images
* An Image may have zero or more Tags

In addition, a Tag is not currently a full-fledged entity, but is instead a simple string.

The following endpoints are provided:

  * [Tagger - create](#tagger-create)

<a name="tagger-create"></a>
## Tagger - create

```
  POST /tagger
```

### Request Payload

```
  {
     <action> : {
      "images" : "[id1, id2, ...]"
      ,"tags"  : "[tag1, tag2, ...]
    }
  }
  
  <action> :: = "create", "add", "replace", "remove"
```

### Description

Assigns a list of tags to a list of Images, or performs various updates to the tags assigned to a list of Images.

The list of potential actions is as follows:

  * **create**: (Not implemented yet, use add for now.) Assigns a list of tags to a list of Images, with the assumption that the Images have not been tagged yet.  Returns an error if the Images has a non-empty list of tags.

  * **replace**: Assigns a list of tags to a list of Images. This is equivalent to the "create" above, but is safe on Images that already contains tags:  all the existing tags will be removed, and the ones passed will replace them.

  * **add**: adds a list of tags to the tags a list of Images.  This action merges/sorts the list of tags passed, to the list of tags that an Image may already have.  For example:  adding the tags ["red","white","blue"] to an image that has the tags ["blue", "green", "yellow"] would result in the image having the tags: ["blue", "green", "red", "white", "yellow"]

  * **remove**: removes a list of tags from the tags of a list of Images.  This action removes the tags passed from the list of tags that an Image may alreday have.  For example, removing the tags ["red","white","blue"] from an image that has the tags ["blue","green","yellow"] would result in the image having the tags: ["green", "yellow"].