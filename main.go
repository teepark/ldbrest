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
	parseFlags()
	openDB()
	defer wo.Close()
	defer ro.Close()
	defer db.Close()

	router := initRouter()
	run(router)
}

func parseFlags() {
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
		req := &struct{ Ops oplist }{}

		err := json.NewDecoder(r.Body).Decode(req)
		if err != nil {
			failErr(w, err)
			return
		}

		err = applyBatch(req.Ops)
		if err == errBadBatch {
			failCode(w, http.StatusBadRequest)
		} else if err != nil {
			failErr(w, err)
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
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

func run(handler http.Handler) {
	if len(serveAddrs) == 0 {
		log.Fatal("no serveaddrs specified!")
	}

	// start up each server in a goroutine of its own
	for _, addr := range serveAddrs {
		if strings.Contains(addr, ":") {
			go http.ListenAndServe(addr, handler)
		} else {
			go func(addr string) {
				l, err := net.Listen("unix", addr)
				if err != nil {
					log.Fatal(err)
				}

				(&http.Server{Handler: handler}).Serve(l)
			}(addr)
		}
	}

	// prevent the main goroutine from ending (and thus the whole process)
	<-make(chan struct{})
}