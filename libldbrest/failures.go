package libldbrest

import (
	"log"
	"net/http"
)

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
