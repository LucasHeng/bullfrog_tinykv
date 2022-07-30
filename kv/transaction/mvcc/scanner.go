package mvcc

import "github.com/pingcap-incubator/tinykv/kv/util/engine_util"

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	WriteIter engine_util.DBIterator
	txn       *MvccTxn
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	WriteIter := txn.Reader.IterCF(engine_util.CfWrite)
	WriteIter.Seek(EncodeKey(startKey, TsMax))
	return &Scanner{
		WriteIter: WriteIter,
		txn:       txn,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	// 关掉迭代器
	scan.WriteIter.Close()
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	for {
		// 迭代器无效
		if !scan.WriteIter.Valid() {
			return nil, nil, nil
		}

		item := scan.WriteIter.Item()
		userKey := DecodeUserKey(item.Key())
		commitTs := decodeTimestamp(item.Key())

		// txn开始的时候还没有提交
		if commitTs >= scan.txn.StartTS {
			scan.WriteIter.Seek(EncodeKey(userKey, commitTs-1))
			continue
		}

		// 看看是否有lock
		lock, err := scan.txn.GetLock(userKey)
		if err != nil {
			return nil, nil, err
		}
		// 看是否锁住
		if lock != nil && lock.Ts < scan.txn.StartTS {
			keyError := new(KeyError)
			keyError.Locked = lock.Info(userKey)
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
		// 不是put，并没有value
		if write.Kind != WriteKindPut {
			scan.WriteIter.Seek(EncodeKey(userKey, 0))
			continue
		}

		keyvalue := EncodeKey(userKey, write.StartTS)
		value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, keyvalue)
		if err != nil {
			return nil, nil, err
		}

		scan.WriteIter.Next()

		return userKey, value, nil
	}
}
