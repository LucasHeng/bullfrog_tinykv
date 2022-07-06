package mvcc

import "github.com/pingcap-incubator/tinykv/kv/util/engine_util"

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	iter engine_util.DBIterator
	txn  *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(startKey)
	return &Scanner{
		iter: iter,
		txn:  txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for {
		if !scan.iter.Valid() {
			return nil, nil, nil
		}

		item := scan.iter.Item()
		key := DecodeUserKey(item.Key())
		commitTs := decodeTimestamp(item.Key())
		// 这个说明有慢于我们当前事务的提交，按照SI的标准是不能读的
		if commitTs >= scan.txn.StartTS {
			scan.iter.Seek(EncodeKey(key, commitTs-1))
			continue
		}

		lock, err := scan.txn.GetLock(key)
		if err != nil {
			return nil, nil, err
		}
		// 读写冲突
		if lock != nil && lock.Ts < scan.txn.StartTS {
			keyError := new(KeyError)
			keyError.Locked = lock.Info(key)
			return nil, nil, keyError
		}
		writeValue, err := item.Value()
		if err != nil {
			return nil, nil, err
		}
		write, err := ParseWrite(writeValue)
		if err != nil {
			return nil, nil, err
		}
		if write.Kind != WriteKindPut {
			// seek to the next smallest key greater than provided.
			scan.iter.Seek(EncodeKey(key, 0))
			continue
		}
		value, err := scan.txn.GetValue(key)
		if err != nil {
			return nil, nil, err
		}
		scan.iter.Next()
		return key, value, nil
	}
}
