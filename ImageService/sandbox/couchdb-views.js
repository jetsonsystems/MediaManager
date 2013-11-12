var views = {
  "by_oid_with_variant": {
    "map": function (doc) {
      if (doc.class_name === 'plm.Image') {
        var key;
        if (doc.orig_id === '') {
          key = [doc.oid, 0, doc.size.width];
          emit(key, doc.path);
        }
        else {
          key = [doc.orig_id, 1, doc.size.width];
          emit(key, doc.name);
        }
      }
    }
  },
  "by_oid_without_variant": {
    "map": function (doc) {
      if (doc.class_name === 'plm.Image') {
        var key;
        if (doc.orig_id === '') {
          key = doc.oid;
          emit(key);
        }
      }
    }
  },
  "by_creation_time": {
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.Image')) {
        var key = date_to_array(doc.created_at);
        if (doc.orig_id === '') {
          key.push(doc.oid,0,doc.name);
          emit(key,doc.path);
        } else {
          key.push(doc.orig_id,1,doc.name);
          emit(key,doc.name);
        }
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    }
  },
  "by_creation_time_tagged": {
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.Image')) {
        var key = date_to_array(doc.created_at);
        if (doc.orig_id === '') {
          if (doc.tags && (doc.tags.length > 0)) {
            key.push(doc.oid,0,doc.name);
            emit(key,doc.path);
          }
        } else {
          key.push(doc.orig_id,1,doc.name);
          emit(key,doc.name);
        }
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    }
  },
  "by_creation_time_untagged": {
    //
    // key:
    // 
    //  original image:
    //
    //    <created time> <id> 0 <doc.name>
    //
    //  variant image:
    //
    //    <created time> <original image id> 1 <doc.name>
    //
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.Image')) {
        var key = date_to_array(doc.created_at);
        if (doc.orig_id === '') {
          if (!doc.tags || (doc.tags.length <= 0)) {
            key.push(doc.oid,0,doc.name);
            emit(key,doc.path);
          }
        } else {
          key.push(doc.orig_id,1,doc.name);
          emit(key,doc.name);
        }
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    }
  },
  "by_creation_time_name": {
    //
    // Only emit for original images.
    //
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.Image')) {
        var key = date_to_array(doc.created_at);
        if (doc.orig_id === '') {
          key.push(doc.name,doc.oid);
          emit(key,doc.name);
        }
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    },
    "reduce": function(keys, values, rereduce) {
      if (rereduce) {
        return sum(values);
      }
      else {
        return values.length;
      }
    }
  },
  "by_creation_time_name_tagged": {
    //
    // Only emit for original images.
    //
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.Image')) {
        var key = date_to_array(doc.created_at);
        if (doc.orig_id === '') {
          if (doc.tags && (doc.tags.length > 0)) {
            key.push(doc.name,doc.oid);
            emit(key,doc.name);
          }
        }
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    },
    "reduce": function(keys, values, rereduce) {
      if (rereduce) {
        return sum(values);
      }
      else {
        return values.length;
      }
    }
  },
  "by_creation_time_name_untagged": {
    //
    // Only emit for original images.
    //
    // key (original image ONLY):
    //
    //    <created time> <doc.name> <id>
    //
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.Image')) {
        var key = date_to_array(doc.created_at);
        if (doc.orig_id === '') {
          if (!doc.tags || (doc.tags.length <= 0)) {
            key.push(doc.name,doc.oid);
            emit(key,doc.path);
          }
        }
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    },
    "reduce": function(keys, values, rereduce) {
      if (rereduce) {
        return sum(values);
      }
      else {
        return values.length;
      }
    }
  },
  "batch_by_ctime": {
    "map": function(doc) {
      if ((!doc.in_trash) && (doc.class_name === 'plm.ImportBatch')) {
        var key = date_to_array(doc.created_at);
        key.push(doc.oid,0);
        emit(key,doc.path);
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth()+1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    }
  },
  "batch_by_oid_w_image": {
    //
    // key: <batch_id>, <original image id>, <0, 1, 2 depending upon whether import, original, or variant>, <name>
    //
    "map": function (doc) {
      var key = [];
      if (doc.class_name === 'plm.ImportBatch') {
        key.push(doc.oid, '0', 0, '');
        emit(key, doc.path);
      }
      if (doc.class_name === 'plm.Image') {
        if (doc.orig_id === '') {
          key.push(doc.batch_id, doc.oid, 1, doc.name);
        }
        else {
          key.push(doc.batch_id, doc.orig_id, 2, doc.name);
        }
        emit(key, doc.name);
      }
    }
  },
  "batch_by_oid_w_image_by_ctime": {
    //
    // key: <batch_id>, <0, 1, 2 depending upon whether import, original or variant>, <in trash>, <date>, <'' or image.name>, <'0' or original image id>
    //  note: Date is 7 fields -> key length is 12.
    //
    "map": function (doc) {
      var key = [];
      if (doc.class_name === 'plm.ImportBatch') {
        key.push(doc.oid, 0, 0, 0, 0, 0, 0, 0, 0, 0, '', '0');
        emit(key, doc.path);
      }
      if (doc.class_name === 'plm.Image') {
        if (doc.orig_id === '') {
          key.push(doc.batch_id, 1);
        }
        else {
          key.push(doc.batch_id, 2);
        }
        if (doc.in_trash) {
          key.push(1);
        }
        else {
          key.push(0);
        }
        var d = date_to_array(doc.created_at);
        key.push.apply(key, d);

        if (doc.orig_id === '') {
          key.push(doc.name, doc.oid);
        }
        else {
          key.push(doc.name, doc.orig_id);
        }

        emit(key, doc.name);
      }
      function date_to_array(aDate) {
        var out = [], d = new Date(aDate);
        out.push(d.getFullYear());
        out.push(d.getMonth() + 1);
        out.push(d.getDate());
        out.push(d.getHours());
        out.push(d.getMinutes());
        out.push(d.getSeconds());
        out.push(d.getMilliseconds());
        return out;
      }
    },
    "reduce": function(keys, values, rereduce) {
      var reduced = {
        num_images: 0,
        num_images_intrash: 0
      };
      if (rereduce) {
        for (var i = 0; i < values.length; i++) {
          var value = values[i];
          reduced.num_images = reduced.num_images + value.num_images;
          reduced.num_images_intrash = reduced.num_images_intrash + value.num_images_intrash;
        }
      }
      else {
        var ni = 0;
        var nit = 0;
        for (var i = 0; i < keys.length; i++) {
          var key = keys[i][0];
          if (key[1] === 1) {
            ni++;
            if (key[2] === 1) {
              nit++;
            }
          }
        }
        reduced.num_images = ni;
        reduced.num_images_intrash = nit;
      }
      return reduced;
    }
  },
  "by_tag": {
    "map" : function(doc) {
      if (!doc.in_trash) {
        if (doc.class_name === 'plm.Image') {
          if (doc.tags) {
            for (var tag in doc.tags) {
              emit(doc.tags[tag],1);
            }
          }
        }
      }
    }
    ,"reduce": function(keys, values) {
      return sum(values);
    }
  }, 
  "by_trash": {
    "map" : "function(doc) { if (doc.in_trash){ emit(doc.oid);} }"
  }
}
