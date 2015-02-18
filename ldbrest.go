package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"github.com/julienschmidt/httprouter"
	"github.com/jmhodges/levigo"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
)

var (
	serveaddr = flag.String(
		"serveaddr",
		"127.0.0.1:8070",
		"[host]:port or /path/to/socketfile of where to run the server",
	)
)

var (
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
)

func main() {
	flag.Parse()

	openDB()
	defer wo.Close()
	defer ro.Close()
	defer db.Close()

	router := initRouter()
	log.Print(run(router))
}

func initRouter() *httprouter.Router {
	router := &httprouter.Router{
		// precision in urls -- I'd rather know when my client is wrong
		RedirectTrailingSlash: false,
		RedirectFixedPath: false,

		HandleMethodNotAllowed: true,
		PanicHandler: handlePanics,
	}

	router.GET("/key/:name", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		b, err := db.Get(ro, []byte(p.ByName("name")))
		if err != nil {
			failErr(w, err)
		} else if b == nil {
			failCode(w, http.StatusNotFound)
			w.WriteHeader(404)
		} else {
			w.Write(b)
		}
	})

	router.PUT("/key/:name", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, r.Body); err != nil {
			failErr(w, err)
			return
		}

		err := db.Put(wo, []byte(p.ByName("name")), buf.Bytes())
		if err != nil {
			failErr(w, err)
		} else {
			w.WriteHeader(204)
		}
	})

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
			w.WriteHeader(204)
		}
	})

	return router
}

func run(router *httprouter.Router) error {
	if strings.Contains(*serveaddr, ":") {
		return http.ListenAndServe(*serveaddr, router)
	}

	listener, err := net.Listen("unix", *serveaddr)
	if err != nil {
		log.Fatal(err)
	}

	return (&http.Server{Handler: router}).Serve(listener)
}

func openDB() {
	args := flag.Args()
	if len(args) == 0 {
		log.Fatal("missing db path cmdline argument")
	}
	path := args[0]

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
	ro := levigo.NewReadOptions()
	ro.SetSnapshot(ss)
	ro.SetFillCache(false)

	it := db.NewIterator(ro)
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
