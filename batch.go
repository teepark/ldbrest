package main

import (
	"errors"
	"github.com/jmhodges/levigo"
)

type oplist []*struct {
	Op, Key, Value string
}

var errBadBatch = errors.New("bad write batch")

func applyBatch(ops oplist) error {
	wb := levigo.NewWriteBatch()
	defer wb.Close()

	for _, op := range ops {
		switch op.Op {
		case "put":
			wb.Put([]byte(op.Key), []byte(op.Value))
		case "delete":
			wb.Delete([]byte(op.Key))
		default:
			return errBadBatch
		}
	}

	return db.Write(wo, wb)
}
