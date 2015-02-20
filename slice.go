package main

import (
	"github.com/jmhodges/levigo"
)

func iterSlice(start, end []byte, handle func([]byte, []byte) error) error {
	ropts := levigo.NewReadOptions()
	ropts.SetFillCache(false)

	it := db.NewIterator(ropts)
	defer it.Close()

	for it.Seek(start); it.Valid(); it.Next() {
		if string(it.Key()) >= string(end) {
			break
		}

		if err := handle(it.Key(), it.Value()); err != nil {
			return err
		}
	}

	return nil
}
