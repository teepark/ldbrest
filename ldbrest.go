package main

import (
	"bytes"
	"flag"
	"github.com/gin-gonic/gin"
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

	engine := initEngine()
	log.Print(run(engine))
}

func initEngine() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())

	engine.GET("/key/:name", func(c *gin.Context) {
		b, err := db.Get(ro, []byte(c.Params.ByName("name")))
		if err != nil {
			c.Fail(500, err)
		} else if b == nil {
			c.AbortWithStatus(404)
		} else {
			c.String(200, string(b))
		}
	})

	engine.PUT("/key/:name", func(c *gin.Context) {
		buf := &bytes.Buffer{}
		if _, err := io.Copy(buf, c.Request.Body); err != nil {
			c.Fail(500, err)
			return
		}

		err := db.Put(wo, []byte(c.Params.ByName("name")), buf.Bytes())
		if err != nil {
			c.Fail(500, err)
		} else {
			c.Writer.WriteHeader(204)
		}
	})

	engine.POST("/snapshot", func(c *gin.Context) {
		req := &struct {
			Destination string
		}{}
		if !c.Bind(req) {
			return
		}

		if err := makeSnap(req.Destination); err != nil {
			c.Fail(500, err)
		} else {
			c.Writer.WriteHeader(204)
		}
	})

	return engine
}

func run(engine *gin.Engine) error {
	if strings.Contains(*serveaddr, ":") {
		return engine.Run(*serveaddr)
	}

	listener, err := net.Listen("unix", *serveaddr)
	if err != nil {
		log.Fatal(err)
	}

	return (&http.Server{Handler: engine}).Serve(listener)
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
