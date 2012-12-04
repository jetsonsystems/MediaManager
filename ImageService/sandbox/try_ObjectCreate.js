var _ = require('underscore');

var Images = {
  _obj: {a: 'A', b: 'B'}
}

var i = Object.create(Images);
console.log("our object keys - " + JSON.stringify(_.keys(i._obj))); 
console.log("our object keys - " + JSON.stringify(_.keys(i.__proto__._obj))); 
console.log(i._obj.a);
