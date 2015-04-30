package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"

	lib "github.com/teepark/ldbrest/libldbrest"
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

func main() {
	parseFlags()

	if flag.NArg() == 0 {
		log.Fatal("missing db path cmdline argument")
	}
	path := flag.Args()[0]

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		lib.OpenDB(path)
		wg.Done()
	}()
	defer lib.CleanupDB()

	router := lib.InitRouter("")
	run(unavailUntilReady(router, wg))
}

func unavailUntilReady(h http.Handler, wg *sync.WaitGroup) http.Handler {
	type maybeHandles struct{ http.Handler }

	notReady := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		http.Error(w, "not finished initing DB", http.StatusServiceUnavailable)
	})

	mh := &maybeHandles{notReady}
	go func() {
		wg.Wait()
		mh.Handler = h
	}()

	return mh
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

func run(router http.Handler) {
	if len(serveAddrs) == 0 {
		serveAddrs = addrlist{"127.0.0.1:7000"}
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
