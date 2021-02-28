package badger

import (
	"bytes"
	"errors"
	"os"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

/*
* tableName,id and field is string type; value is []byte type
* all string type's parameter can't include delimiter
* write: not exist will be create, exist will be overwritten
* read : not exist or error will return nil
 */

// Handle db handle
type Handle = *badger.DB

// delimiter
const d string = "`"

var errStr error = errors.New("parameter can not include character: " + d)
var err error

// OpenDb open db
func OpenDb(path string, password ...string) (Handle, error) {
	fi, err := os.Stat(path)
	if err != nil || os.IsNotExist(err) || !fi.IsDir() {
		if os.IsNotExist(err) || !fi.IsDir() { // floder not exist
			err = os.MkdirAll(path, os.FileMode(os.O_CREATE)|os.FileMode(os.O_RDWR))
			if err != nil {
				return nil, err
			}
		} else { // such as permissions...
			return nil, err
		}
	}

	var opts badger.Options
	opts = badger.DefaultOptions(path)
	opts = opts.WithLoggingLevel(badger.ERROR) // log level

	if len(password) != 0 {
		opts.EncryptionKey = []byte(password[0]) //encypto
	}
	opts.ValueLogFileSize = 1 << 29 //512MB
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	return db, err
}

// CloseDb close db handle
func CloseDb(h Handle) error {
	return h.Close()
}

func checkkey(ks ...string) bool {
	for _, k := range ks {
		for _, v := range k {
			if string(v) == d {
				return false
			}
		}
	}
	return true
}

/*
* base operation( original key/value )
 */

// SetKey set or updata key-value
func SetKey(key string, value []byte, h Handle, ttl ...time.Duration) error {
	if !checkkey(key) {
		return errStr
	}
	txn := h.NewTransaction(true)
	defer txn.Discard()

	if len(ttl) == 0 {
		if err = txn.Set([]byte(key), value); err != nil {
			return err
		}
	} else {
		if err = txn.SetEntry(
			badger.NewEntry([]byte(key), value).WithTTL(ttl[0])); err != nil {
			return err
		}
	}
	return txn.Commit()
}

// DeleteKey delete a key-value
func DeleteKey(key string, h Handle) error {
	if !checkkey(key) {
		return errStr
	}
	txn := h.NewTransaction(true)
	defer txn.Discard()

	if err = txn.Delete([]byte(key)); err != nil {
		return err
	}
	return txn.Commit()
}

// GetValue get a value by key, key not exist will return nil
func GetValue(key string, h Handle) []byte {
	txn := h.NewTransaction(false)
	defer txn.Discard()
	item, err := txn.Get([]byte(key))
	if err != nil {
		return []byte("")
	}

	err = txn.Commit()
	if err != nil {
		return []byte("")
	}

	valCopy, err := item.ValueCopy(nil)
	if err != nil {
		return []byte("")
	}
	return valCopy
}

/*
* Table operation, Prefix achieve tables.
* get a table's value need parameter: tableName,field,id;
* key = tableName`id`field (` is delimiter, as PRIMARY_KEY in sql )
 */

// CreatTable create/set a table; exist will be overwritten
func CreatTable(tableName, id string, fv map[string][]byte, h Handle, ttl ...time.Duration) error {
	if !checkkey(tableName, id) {
		return errStr
	}
	txn := h.NewTransaction(true)
	defer txn.Discard()

	for f, v := range fv {
		if !checkkey(f) {
			return errStr
		}
		if len(ttl) == 0 {
			if err = txn.Set([]byte(tableName+d+id+d+f), v); err != nil {
				return err
			}
		} else {
			if err = txn.SetEntry(badger.NewEntry([]byte(tableName+d+id+d+f), v).WithTTL(ttl[0])); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}

// SetTableValue set/updata a table's value
func SetTableValue(tableName, id, field string, value []byte, h Handle, ttl ...time.Duration) error {
	if !checkkey(tableName, field, id) {
		return errStr
	}
	txn := h.NewTransaction(true)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	if len(ttl) == 0 {
		if err = txn.Set([]byte(tableName+d+id+d+field), []byte(value)); err != nil {
			return err
		}
	} else {
		if err = txn.SetEntry(badger.NewEntry([]byte(tableName+d+id+d+field), []byte(value)).WithTTL(ttl[0])); err != nil {
			return err
		}
	}
	it.Close()
	return txn.Commit()
}

// ExistTable detect tableName is exist
func ExistTable(tableName string, h Handle) bool {
	txn := h.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d)

	R := false
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		R = true
	}
	return R
}

// ExistTableRecord detect table has id record
func ExistTableRecord(tableName, id string, h Handle) bool {
	txn := h.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d + id + d)

	R := false
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		R = true
	}
	return R
}

// GetTable get a table's all values
func GetTable(tableName, id string, h Handle) map[string][]byte {

	txn := h.NewTransaction(false) // 新建只读事务
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d + id + d)
	preLen := len(prefix)
	R := make(map[string][]byte)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		f := item.KeyCopy(nil)[preLen:]

		v, err := item.ValueCopy(nil)
		if err != nil {
			r := make(map[string][]byte)
			return r
		}

		R[string(f)] = v
	}
	return R
}

// GetTableValue get one specify value
func GetTableValue(tableName, id, field string, h Handle) []byte {
	txn := h.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d + id + d + field)
	preLen := len(prefix)
	var R []byte

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()

		k := item.KeyCopy(nil)
		k = k[preLen:]

		v, err := item.ValueCopy(nil)
		if err != nil {
			// log err
			return nil
		}
		if string(k) == id {
			R = v
			break
		}

	}
	return R
}

// EqualTableValue check the specify value is equal judgeValue
func EqualTableValue(tableName, id, field string, judgeValue []byte, h Handle) bool {
	if bytes.Equal(judgeValue, GetTableValue(tableName, id, field, h)) {
		return true
	}
	return false
}
