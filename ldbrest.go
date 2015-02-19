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

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/jmhodges/levigo"
	"github.com/julienschmidt/httprouter"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

// addrlist to support the flag.Value interface
// and support multiple "serveaddr"s
type addrlist []string

func (al *addrlist) String() string {
	return strings.Join(*al, ", ")
}

func (al *addrlist) Set(addr string) error {
	*al = append(*al, addr)
	return nil
}

// serveAddrs is the addrlist that captures -s and -serveaddr flags
var serveAddrs addrlist

// global vars for the main leveldb accesses
var (
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
)

func main() {
	// direct -s and -serveaddr flags at serveAddrs
	flag.Var(
		&serveAddrs,
		"s",
		"[host]:port or /path/to/socket of where to run the server. may be provided more than once",
	)
	flag.Var(
		&serveAddrs,
		"serveaddr",
		"[host]:port or /path/to/socket of where to run the server. may be provided more than once",
	)
	flag.Parse()

	openDB()
	defer wo.Close()
	defer ro.Close()
	defer db.Close()

	router := initRouter()
	run(router)
}

func initRouter() *httprouter.Router {
	router := &httprouter.Router{
		// precision in urls -- I'd rather know when my client is wrong
		RedirectTrailingSlash: false,
		RedirectFixedPath:     false,

		HandleMethodNotAllowed: true,
		PanicHandler:           handlePanics,
	}

	// retrieve single keys
	router.GET("/key/*name", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		b, err := db.Get(ro, []byte(p.ByName("name")[1:]))
		if err != nil {
			failErr(w, err)
		} else if b == nil {
			failCode(w, http.StatusNotFound)
		} else {
			w.Header().Set("Content-Type", "text/plain")
			w.Write(b)
		}
	})

	// set single keys (value goes in the body)
	router.PUT("/key/*name", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, r.Body); err != nil {
			failErr(w, err)
			return
		}

		err := db.Put(wo, []byte(p.ByName("name")[1:]), buf.Bytes())
		if err != nil {
			failErr(w, err)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	// delete a key by name
	router.DELETE("/key/*name", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		err := db.Delete(wo, []byte(p.ByName("name")[1:]))
		if err != nil {
			failErr(w, err)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	// fetch a contiguous range of keys and their values
	router.GET("/slice", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")

		if start == "" || end == "" {
			failCode(w, http.StatusBadRequest)
			return
		}

		type keyval struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}

		type wrapper struct {
			Length int       `json:"length"`
			Data   []*keyval `json:"data"`
		}

		ropts := levigo.NewReadOptions()
		ropts.SetFillCache(false)
		it := db.NewIterator(ropts)
		results := make([]*keyval, 0)
		for it.Seek([]byte(start)); it.Valid(); it.Next() {
			if string(it.Key()) >= end {
				break
			}

			results = append(results, &keyval{
				string(it.Key()), string(it.Value()),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&wrapper{len(results), results})
	})

	// atomically write a batch of updates
	router.POST("/batch", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		req := &struct{
			Ops []*struct {
				Op, Key, Value string
			}
		}{}

		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			failErr(w, err)
			return
		}

		wb := levigo.NewWriteBatch()
		defer wb.Close()

		for _, op := range req.Ops {
			switch op.Op {
			case "put":
				wb.Put([]byte(op.Key), []byte(op.Value))
			case "delete":
				wb.Delete([]byte(op.Key))
			default:
				failCode(w, http.StatusBadRequest)
				return
			}
		}

		if err := db.Write(wo, wb); err != nil {
			failErr(w, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	// get a leveldb property
	router.GET("/property/:name", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		prop := db.PropertyValue(p.ByName("name"))
		if prop == "" {
			failCode(w, http.StatusNotFound)
		} else {
			w.Header().Set("Content-Type", "text/plain")
			w.Write([]byte(prop))
		}
	})

	// copy the whole db via a point-in-time snapshot
	router.POST("/snapshot", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		req := &struct {
			Destination string
		}{}
		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			failErr(w, err)
			return
		}

		if err := makeSnap(req.Destination); err != nil {
			failErr(w, err)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})

	return router
}

func run(router *httprouter.Router) {
	if len(serveAddrs) == 0 {
		log.Fatal("no serveaddrs specified!")
	}

	// start up each server in a goroutine of its own
	for _, addr := range serveAddrs {
		if strings.Contains(addr, ":") {
			go http.ListenAndServe(addr, router)
		} else {
			go func(addr string) {
				l, err := net.Listen("unix", addr)
				if err != nil {
					log.Fatal(err)
				}

				(&http.Server{Handler: router}).Serve(l)
			}(addr)
		}
	}

	// prevent the main goroutine from ending (and thus the whole process)
	<-make(chan struct{})
}

func openDB() {
	if flag.NArg() == 0 {
		log.Fatal("missing db path cmdline argument")
	}
	path := flag.Args()[0]

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	ldb, err := levigo.Open(path, opts)
	if err != nil {
		log.Fatalf("opening leveldb: %s", err)
	}

	db = ldb
	ro = levigo.NewReadOptions()
	wo = levigo.NewWriteOptions()
}

func makeSnap(dest string) error {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	opts.SetErrorIfExists(true)
	to, err := levigo.Open(dest, opts)
	if err != nil {
		return err
	}
	defer to.Close()

	ss := db.NewSnapshot()
	sro := levigo.NewReadOptions()
	sro.SetSnapshot(ss)
	sro.SetFillCache(false)

	it := db.NewIterator(sro)
	defer it.Close()

	wb := levigo.NewWriteBatch()

	var i uint
	for it.SeekToFirst(); it.Valid(); it.Next() {
		wb.Put(it.Key(), it.Value())
		i++

		if i%1000 == 0 {
			wb, err = dumpBatch(wb, to, true)
			if err != nil {
				goto fail
			}
		}
	}

	if i%1000 != 0 {
		_, err = dumpBatch(wb, to, false)
		if err != nil {
			goto fail
		}
	}

	return nil

fail:
	levigo.DestroyDatabase(dest, opts)
	return err
}

func dumpBatch(wb *levigo.WriteBatch, dest *levigo.DB, more bool) (*levigo.WriteBatch, error) {
	defer wb.Close()

	err := dest.Write(wo, wb)
	if err != nil {
		return nil, err
	}

	if more {
		return levigo.NewWriteBatch(), nil
	}
	return nil, nil
}

func handlePanics(w http.ResponseWriter, r *http.Request, err interface{}) {
	log.Printf("PANIC in handler: %s", err)
	w.WriteHeader(http.StatusInternalServerError)
}

func failErr(w http.ResponseWriter, err error) {
	log.Print(err)
	w.WriteHeader(http.StatusInternalServerError)
}

func failCode(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
	w.Write([]byte(http.StatusText(code)))
}
