package main

import "github.com/jmhodges/levigo"

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
	sro := levigo.NewReadOptions()
	sro.SetSnapshot(ss)
	sro.SetFillCache(false)

	it := db.NewIterator(sro)
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
