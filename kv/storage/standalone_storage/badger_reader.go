package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type BadgerReader struct {
	Txn *badger.Txn
}

func NewBadgerReader(db *badger.DB) *BadgerReader {
	txn := db.NewTransaction(false)
	return &BadgerReader{
		Txn: txn,
	}
}

func (b *BadgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	data, err := engine_util.GetCFFromTxn(b.Txn, cf, key)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			// it is not error
			return nil, nil
		}
		return nil, err
	}
	return data, err
}

func (b *BadgerReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, b.Txn)
}

func (b *BadgerReader) Close() {
	b.Txn.Discard()
}
