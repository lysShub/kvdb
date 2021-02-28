package badger

import (
	"bytes"
	"errors"
	"os"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

/*
* tableName、id and field type is string; value type is []byte
* all string type's parameter can't include delimiter
* write: not exist will be create, exist will be rewrite
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
func SetKey(key string, value []byte, dbHandle Handle, surTime ...time.Duration) error {
	if !checkkey(key) {
		return errStr
	}
	txn := dbHandle.NewTransaction(true)
	defer txn.Discard()

	if len(surTime) == 0 {
		if err = txn.Set([]byte(key), value); err != nil {
			return err
		}
	} else {
		if err = txn.SetEntry(
			badger.NewEntry([]byte(key), value).WithTTL(surTime[0])); err != nil {
			return err
		}
	}
	return txn.Commit()
}

// DeleteKey delete a key-value
func DeleteKey(key string, dbHandle Handle) error {
	if !checkkey(key) {
		return errStr
	}
	txn := dbHandle.NewTransaction(true)
	defer txn.Discard()

	if err = txn.Delete([]byte(key)); err != nil {
		return err
	}
	return txn.Commit()
}

// GetValue get a value by key, key not exist will return nil
func GetValue(key string, dbHandle Handle) []byte {
	txn := dbHandle.NewTransaction(false)
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
* get a table's value need: tableName,field,id;
* so the key = tableName`id`field (` is delimiter)
 */

// CreatTable create/set a table; exist will be rewrite
func CreatTable(tableName, id string, fv map[string][]byte, dbHandle Handle, surTime ...time.Duration) error {
	if !checkkey(tableName, id) {
		return errStr
	}
	txn := dbHandle.NewTransaction(true)
	defer txn.Discard()

	for f, v := range fv {
		if !checkkey(f) {
			return errStr
		}
		if len(surTime) == 0 {
			if err = txn.Set([]byte(tableName+d+id+d+f), v); err != nil {
				return err
			}
		} else {
			if err = txn.SetEntry(badger.NewEntry([]byte(tableName+d+id+d+f), v).WithTTL(surTime[0])); err != nil {
				return err
			}
		}
	}

	return txn.Commit()
}

// SetTableValue set/updata a table's value
func SetTableValue(tableName, field, id string, value []byte, dbHandle Handle, surTime ...time.Duration) error {
	if !checkkey(tableName, field, id) {
		return errStr
	}
	txn := dbHandle.NewTransaction(true)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	if len(surTime) == 0 {
		if err = txn.Set([]byte(tableName+d+id+d+field), []byte(value)); err != nil {
			return err
		}
	} else {
		if err = txn.SetEntry(badger.NewEntry([]byte(tableName+d+id+d+field), []byte(value)).WithTTL(surTime[0])); err != nil {
			return err
		}
	}
	it.Close()
	return txn.Commit()
}

// ExistTable detect tableName is exist
func ExistTable(tableName string, dbHandle Handle) bool {
	txn := dbHandle.NewTransaction(false)
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

// ExistTableField detect tableName`id is exist
func ExistTableField(tableName, id string, dbHandle Handle) bool {
	txn := dbHandle.NewTransaction(false)
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
func GetTable(tableName, id string, dbHandle Handle) map[string][]byte {

	txn := dbHandle.NewTransaction(false) // 新建只读事务
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

// GetTableValue 获取table的某个值
func GetTableValue(tableName, key string, dbHandle Handle) []byte {
	txn := dbHandle.NewTransaction(false) // 新建只读事务
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d)
	preLen := len(prefix)
	var R []byte

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()

		k := item.KeyCopy(nil)
		k = k[preLen:]

		v, err = item.ValueCopy(nil)
		if err != nil {
			return nil
		}
		if string(k) == key {
			R = v
			break
		}

	}
	return R
}

// EqualTableValue 表中某个值是否相等，不存在\出错返回false
func EqualTableValue(tableName, key string, judgeValue []byte, dbHandle Handle) bool {
	if bytes.Equal(judgeValue, GetTableValue(tableName, key, dbHandle)) {
		return true
	}
	return false
}
