package libldbrest

import (
	"encoding/json"
	"fmt"
	"github.com/jmhodges/levigo"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestKeyPutGet(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	app := newAppTester(t)

	app.put("foo", "bar")
	val := app.get("foo")

	if val != "bar" {
		t.Fatalf("wrong 'foo' value: %s", val)
	}

	found, _ := app.maybeGet("baz")
	if found {
		t.Fatal("found 'baz' when we shouldn't have")
	}
}

func TestIteration(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	app := newAppTester(t)

	app.put("a", "A")
	app.put("b", "B")
	app.put("c", "C")
	app.put("d", "D")

	/*
		keys only [b, d)
	*/
	rr := app.doReq("GET", "http://domain/iterate?start=b&include_start=yes&end=d&include_values=no", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kresp := &struct {
		More bool
		Data []string
	}{}
	if err := json.NewDecoder(rr.Body).Decode(kresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kresp.Data) == 2, "wrong # of returned keys: %d", len(kresp.Data))
	assert(t, kresp.Data[0] == "b", "wrong returned key: %s", kresp.Data[0])
	assert(t, kresp.Data[1] == "c", "wrong returned key: %s", kresp.Data[1])
	assert(t, !kresp.More, "ldbrest falsely reporting 'more'")

	/*
		keys and vals [0, 2)
	*/
	rr = app.doReq("GET", "http://domain/iterate?max=2", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kvresp := &struct {
		More bool
		Data []*struct {
			Key   string
			Value string
		}
	}{}
	if err := json.NewDecoder(rr.Body).Decode(kvresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kvresp.Data) == 2, "wrong # of keyvals: %d", len(kvresp.Data))
	assert(t, kvresp.Data[0].Key == "a", "wrong first key: %s", kvresp.Data[0].Key)
	assert(t, kvresp.Data[0].Value == "A", "wrong first value: %s", kvresp.Data[0].Value)
	assert(t, kvresp.Data[1].Key == "b", "wrong second key: %s", kvresp.Data[1].Key)
	assert(t, kvresp.Data[1].Value == "B", "wrong second value: %s", kvresp.Data[1].Value)
	assert(t, !kvresp.More, "ldbrest falsely reporting 'more'")

	/*
		keys and vals [a, d] with max 3 (trigger 'more')
	*/
	rr = app.doReq("GET", "http://domain/iterate?start=a&end=d&include_end=yes&max=3", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kvresp.More = false
	kvresp.Data = nil
	if err := json.NewDecoder(rr.Body).Decode(kvresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kvresp.Data) == 3, "wrong # of keyvals: %d", len(kvresp.Data))
	assert(t, kvresp.More, "'more' should be true")
	assert(t, kvresp.Data[0].Key == "a", "wrong data[0].Key: %s", kvresp.Data[0].Key)
	assert(t, kvresp.Data[1].Key == "b", "wrong data[1].Key: %s", kvresp.Data[1].Key)
	assert(t, kvresp.Data[2].Key == "c", "wrong data[2].Key: %s", kvresp.Data[2].Key)
	assert(t, kvresp.Data[0].Value == "A", "wrong data[0].Value: %s", kvresp.Data[0].Value)
	assert(t, kvresp.Data[1].Value == "B", "wrong data[1].Value: %s", kvresp.Data[1].Value)
	assert(t, kvresp.Data[2].Value == "C", "wrong data[2].Value: %s", kvresp.Data[2].Value)

	/*
		keys only [d, a] in reverse with max 2 (trigger 'more')
	*/
	rr = app.doReq("GET", "http://domain/iterate?start=d&forward=no&max=2&end=a&include_end=yes&include_values=no", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kresp.More = false
	kresp.Data = nil
	if err := json.NewDecoder(rr.Body).Decode(kresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kresp.Data) == 2, "wrong # of keys: %d", len(kresp.Data))
	assert(t, kresp.More, "'more' should be true (reverse)")
	assert(t, kresp.Data[0] == "d", "wrong data[0]: %s", kresp.Data[0])
	assert(t, kresp.Data[1] == "c", "wrong data[1]: %s", kresp.Data[1])
}

func setup(tb testing.TB) string {
	dirpath, err := ioutil.TempDir("", "ldbrest_test")
	if err != nil {
		tb.Fatal(err)
	}

	opts := levigo.NewOptions()
	defer opts.Close()

	opts.SetCreateIfMissing(true)
	opts.SetErrorIfExists(true)

	db, err = levigo.Open(dirpath, opts)
	if err != nil {
		os.RemoveAll(dirpath)
		tb.Fatal(err)
	}

	ro = levigo.NewReadOptions()
	wo = levigo.NewWriteOptions()

	return dirpath
}

func cleanup(path string) {
	if db != nil {
		db.Close()
	}
	if ro != nil {
		ro.Close()
	}
	if wo != nil {
		wo.Close()
	}
	os.RemoveAll(path)
}

func assert(tb testing.TB, cond bool, msg string, args ...interface{}) {
	if !cond {
		tb.Fatalf(msg, args...)
	}
}

type appTester struct {
	app http.Handler
	tb  testing.TB
}

func newAppTester(tb testing.TB) *appTester {
	return &appTester{app: initRouter(), tb: tb}
}

func (app *appTester) doReq(method, url, body string) *httptest.ResponseRecorder {
	var bodyReader io.Reader
	if body == "" {
		bodyReader = nil
	} else {
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		app.tb.Fatal(err)
	}

	rr := httptest.NewRecorder()
	app.app.ServeHTTP(rr, req)
	rr.Flush()
	return rr
}

func (app *appTester) put(key, value string) {
	rr := app.doReq("PUT", fmt.Sprintf("http://domain/key/%s", key), value)
	if rr.Code != 204 {
		app.tb.Fatalf("non-204 PUT /key/X response: %d", rr.Code)
	}
}

func (app *appTester) maybeGet(key string) (bool, string) {
	rr := app.doReq("GET", fmt.Sprintf("http://domain/key/%s", key), "")

	switch rr.Code {
	case 404:
		return false, ""
	case 200:
		ct := rr.HeaderMap.Get("Content-Type")
		if ct != "text/plain" {
			app.tb.Fatalf("non 'text/plain' 200 GET /key/%s response: %s", key, ct)
		}
	default:
		app.tb.Fatalf("questionable GET /key/%s response: %d", key, rr.Code)
	}

	return true, rr.Body.String()
}

func (app *appTester) get(key string) string {
	found, value := app.maybeGet(key)
	if !found {
		app.tb.Fatalf("failed to find key %s", key)
	}
	return value
}
