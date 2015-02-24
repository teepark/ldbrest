/*
ldbrest is a simple REST server for exposing a leveldb[1] database over TCP.

Leveldb is a key-value database written to be embedded. Its major trade-off
from an operational standpoint is that a single database can only be open
*for reading OR writing* by a single process at a time.

These properties make it perfect for a simple REST server offering CRUD
operations on keys. ldbrest exposes a few other useful endpoints as well.

It is invoked with an optional -s/-serveaddr flag and a required positional
/path/to/leveldb. "serveaddr" can be a "host:port" for TCP or a
/path/to/socketfile for a streaming unix domain socket and can be given more
than once. Without any -s/-serveaddr flags it will serve on "127.0.0.1:7000".

The server offers these endpoints:

  GET /key/<name>
Returns the value associated with the <name> key in the response body with
content-type text/plain (or 404s).

  PUT /key/<name>
Takes the (unparsed) request body and stores it as the value under key <name>
and returns a 204.

  DELETE /key/<name>
Deletes the key <name> and returns a 204.

  GET /iterate
Iterates over the sorted keys. It takes optional query string parameters to
control the iterator:

* "forward" is whether to iterate forward through sorted order or reverse
(default "yes", iterate forward)

* "start" is a key to start from (default beginning/end)

* "include_start" is whether to include the key precisely matching "start" if
it exists (default "yes")

* "end" is the key at which to terminate iteration (defaults to end/beginning)

* "include_end" is whether to include the key precisely matching "end" if it
exists (default "no")

* "max" is a maximum number of keys(/values) to return, this can be provided
in conjunction with "end" in which case either condition would terminate
iteration (default 1000, higher values than this will be ignored)

* "include_values" is whether to produce {"key": "<key>", "value": "<value>"}
objects or just "<key>" strings (default "yes")

It then returns a JSON object with two keys "more" and "data". "data" is an
array of either objects or strings depending on "include_values", while "more"
is false unless "end" was provided but "max" caused the end of iteration (there
was still more to go before we would have hit "end").

  POST /batch
Applies a batch of updates atomically. It accepts a JSON request body with key
"ops", an array of objects with keys "op", "key", and "value". "op" may be
"put" or "delete", in the latter case "value" may be omitted.

  GET /property/<name>
Gets and returns the leveldb property in the text/plain 200 response body, or
404s if it isn't a valid property name.

  POST /snapshot
Needs a JSON request body with key "destination", which should be a file system
path. ldbrest will make a complete copy of the database at that location, then
return a 204 (after what might be a while).

[1] https://github.com/google/leveldb
*/
package main
