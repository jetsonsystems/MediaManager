var d = new Date();
var datestring = d.toISOString();
var d2 = new Date(datestring);

console.log("d.toJSON(): " + d.toJSON());
console.log("d.toISOString(): " + datestring);
console.log("d2.toISOString(): "+ d2.toISOString());


