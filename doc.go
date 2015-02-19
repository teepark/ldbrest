/*
ldbrest is a simple REST server for exposing a leveldb[1] database over TCP.

Leveldb is a key-value database written to be embedded. Its major trade-off
from an operational standpoint is that a single database can only be open
*for reading OR writing* by a single process at a time.

These properties make it perfect for a simple REST server offering CRUD
operations on keys. ldbrest exposes a few other useful endpoints as well.

GET /key/<name> returns the value associated with the <name> key in the
response body with content-type text/plain (or 404s).

PUT /key/<name> takes the (unparsed) request body and stores it as the value
under key <name> and returns a 204.

DELETE /key/<name> deletes the key <name> and returns a 204.

GET /slice needs "start" and "end" querystring parameters. It will return a
200 response with a JSON body with keys "length" and "data". data is an
array of objects with "key" and "value" strings, "length" is just the length
of "data". The returned key/value pairs will be all those in the database
between "start" and "end" in sorted order.

POST /batch accepts a JSON request body with key "ops", an array of objects
with keys "op", "key", and "value". "op" may be "put" or "delete", in the
latter case "value" may be omitted.

GET /property/<name> gets and returns the leveldb property in the text/plain
200 response body, or 404s if it isn't a valid property name.

POST /snapshot needs a JSON request body with key "destination", which
should be a file system path. ldbrest will make a complete copy of the
database at that location, then return a 204 (after what might be a while).

[1] https://github.com/google/leveldb
*/
package main
