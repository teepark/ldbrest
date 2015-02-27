package libldbrest

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/jmhodges/levigo"
	"github.com/julienschmidt/httprouter"
)

const (
	ABSMAX = 1000
)

var (
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
)

func OpenDB() {
	if flag.NArg() == 0 {
		log.Fatal("missing db path cmdline argument")
	}
	path := flag.Args()[0]

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	defer opts.Close()
	ldb, err := levigo.Open(path, opts)
	if err != nil {
		log.Fatalf("opening leveldb: %s", err)
	}

	db = ldb
	ro = levigo.NewReadOptions()
	wo = levigo.NewWriteOptions()
}

func CleanupDB() {
	wo.Close()
	ro.Close()
	db.Close()
}

func InitRouter() *httprouter.Router {
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
	router.GET("/iterate", func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		q := r.URL.Query()
		start := q.Get("start")
		end := q.Get("end")

		var (
			max int
			err error
		)
		maxs := q.Get("max")
		if maxs == "" {
			max = ABSMAX
		} else if max, err = strconv.Atoi(maxs); err != nil {
			failErr(w, err)
			return
		}
		if max > ABSMAX {
			max = ABSMAX
		}

		// by default we traverse forwards,
		// include "start" but not "end" (like go slicing),
		// and include values in the response data
		ignore_start := q.Get("include_start") == "no"
		include_end := q.Get("include_end") == "yes"
		backwards := q.Get("forward") == "no"
		skip_values := q.Get("include_values") == "no"

		type keyval struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		type wrapper struct {
			More bool          `json:"more"`
			Data []interface{} `json:"data"` // either keyvals or just string keys
		}

		var (
			data = make([]interface{}, 0)
			more bool
		)

		once := func(key, value []byte) error {
			if skip_values {
				data = append(data, string(key))
			} else {
				data = append(data, &keyval{string(key), string(value)})
			}
			return nil
		}

		if end == "" {
			err = iterateN([]byte(start), max, !ignore_start, backwards, once)
			more = false
		} else {
			more, err = iterateUntil([]byte(start), []byte(end), max, !ignore_start, include_end, backwards, once)
		}

		if err != nil {
			failErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&wrapper{more, data})
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
