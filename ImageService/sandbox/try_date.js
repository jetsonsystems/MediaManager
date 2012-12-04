var d = new Date();
var datestring = d.toISOString();
var d2 = new Date(datestring);

console.log("d.toJSON(): " + d.toJSON());
console.log("d.toISOString(): " + datestring);
console.log("d2.toISOString(): "+ d2.toISOString());

console.log('typeof datestring: %j', typeof datestring);
console.log('typeof d2: %j', typeof d2);

console.log('datestring instanceof String: %j', datestring instanceof String);
console.log('d2 instanceof Date: %j', d2 instanceof Date);

function date_to_array(aDate) {
  var out = [];
  out.push(aDate.getFullYear());
  out.push(aDate.getMonth()+1);
  out.push(aDate.getDate());
  out.push(aDate.getHours());
  out.push(aDate.getMinutes());
  out.push(aDate.getSeconds());
  out.push(aDate.getMilliseconds());
  // out.push(aDate.getTimezoneOffset());
  return out;
}

console.log("d to array: %j", date_to_array(d));


