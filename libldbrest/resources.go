package libldbrest

import (
	"log"

	"github.com/jmhodges/levigo"
)

var (
	db *levigo.DB
	ro *levigo.ReadOptions
	wo *levigo.WriteOptions
)

// OpenDB intializes global vars for the leveldb database.
// Be sure and call CleanupDB() to free those resources.
func OpenDB(dbpath string) {
	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(true)
	defer opts.Close()
	ldb, err := levigo.Open(dbpath, opts)
	if err != nil {
		log.Fatalf("opening leveldb: %s", err)
	}

	db = ldb
	ro = levigo.NewReadOptions()
	wo = levigo.NewWriteOptions()
}

// CleanupDB frees the global vars associated with the open leveldb.
func CleanupDB() {
	wo.Close()
	ro.Close()
	db.Close()
	wo = nil
	ro = nil
	db = nil
}
