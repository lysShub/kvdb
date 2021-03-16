package badger

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"time"

	badger "github.com/dgraph-io/badger/v2"
)

// Handle db handle
type Handle = *badger.DB

// Badger badgerdb
// badgerdb中没有表的概念，使用前缀实现，使用Delimiter区分前缀和字段
type Badger struct {
	DbHandle  Handle   //必须，数据库句柄
	Path      string   //储存路径，默认路径文当前路径db文件夹
	Password  [16]byte //密码，默认无密码
	RAM       bool     //内存模式，默认false
	Delimiter string   //分割符，默认为字符`
}

var errStr error = errors.New("can not include delimiter character")
var err error

// OpenDb open db
func (d *Badger) OpenDb() error {
	if d.Path != "" { // 设置路径
		fi, err := os.Stat(d.Path)
		if err != nil {
			if os.IsNotExist(err) {
				if err = os.MkdirAll(d.Path, os.FileMode(os.O_CREATE)|os.FileMode(os.O_RDWR)); err != nil {
					return err
				}
			} else {
				return err
			}
		} else if !fi.IsDir() { //存在同名文件
			if err = os.MkdirAll(d.Path, os.FileMode(os.O_CREATE)|os.FileMode(os.O_RDWR)); err != nil {
				return err
			}
		}
	} else { // 设置为默认路径
		var path string = filepath.ToSlash(filepath.Dir(os.Args[0])) + `/db/`
		if err := os.MkdirAll(path, os.FileMode(os.O_CREATE)|os.FileMode(os.O_RDWR)); err != nil {
			return err
		}
		d.Path = path
	}

	var opts badger.Options
	opts = badger.DefaultOptions(d.Path)
	opts = opts.WithLoggingLevel(badger.ERROR)

	if d.RAM {
		opts.InMemory = true
	}
	if d.Password[:] != nil {
		opts.EncryptionKey = d.Password[:]
	}
	if d.Delimiter == "" {
		d.Delimiter = "`"
	}
	opts.ValueLogFileSize = 1 << 29 //512MB
	opts.Dir = d.Path
	opts.ValueDir = d.Path
	db, err := badger.Open(opts)
	d.DbHandle = db
	return err
}

// CloseDb close
func (d *Badger) CloseDb() error {
	return d.DbHandle.Close()
}

func (d *Badger) checkkey(ks ...string) bool {
	for _, k := range ks {
		for _, v := range k {
			if string(v) == d.Delimiter {
				return false
			}
		}
	}
	return true
}

// key/value

// SetKey
func (d *Badger) SetKey(key string, value []byte, ttl ...time.Duration) error {
	if !d.checkkey(key) {
		return errStr
	}
	txn := d.DbHandle.NewTransaction(true)
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

// DeleteKey
func (d *Badger) DeleteKey(key string, h Handle) error {
	if !d.checkkey(key) {
		return errStr
	}
	txn := h.NewTransaction(true)
	defer txn.Discard()

	if err = txn.Delete([]byte(key)); err != nil {
		return err
	}
	return txn.Commit()
}

// ReadKey
func (d *Badger) ReadKey(key string) []byte {
	txn := d.DbHandle.NewTransaction(false)
	defer txn.Discard()

	var item *badger.Item
	if item, err = txn.Get([]byte(key)); err != nil {
		return nil
	}

	if err = txn.Commit(); err != nil {
		return nil
	}

	var valCopy []byte
	if valCopy, err = item.ValueCopy(nil); err != nil {
		return nil
	}
	return valCopy
}

// table

// SetTable
func (d *Badger) SetTable(tableName string, t map[string]map[string][]byte, ttl ...time.Duration) error {

	txn := d.DbHandle.NewTransaction(true)
	defer txn.Discard()

	for id, kv := range t {
		if !d.checkkey(id) {
			return errStr
		}
		for k, v := range kv {
			if len(ttl) == 0 {
				if err = txn.Set([]byte(tableName+d.Delimiter+id+d.Delimiter+k), v); err != nil {
					return err
				}
			} else {
				if err = txn.SetEntry(badger.NewEntry([]byte(tableName+d.Delimiter+id+d.Delimiter+k), v).WithTTL(ttl[0])); err != nil {
					return err
				}
			}
		}
	}
	return txn.Commit()
}

// SetTableRow
func (d *Badger) SetTableRow(tableName, id string, kv map[string][]byte, ttl ...time.Duration) error {
	txn := d.DbHandle.NewTransaction(true)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	for k, v := range kv {
		if len(ttl) == 0 {
			if err = txn.Set([]byte(tableName+d.Delimiter+id+d.Delimiter+k), []byte(v)); err != nil {
				return err
			}
		} else {
			if err = txn.SetEntry(badger.NewEntry([]byte(tableName+d.Delimiter+id+d.Delimiter+k), []byte(v)).WithTTL(ttl[0])); err != nil {
				return err
			}
		}
	}
	return txn.Commit()
}

func (d *Badger) SetTableValue(tableName, id, field string, value []byte, ttl ...time.Duration) error {

	return nil
}

// ExistTable detect tableName is exist
func (d *Badger) ExistTable(tableName string) bool {
	txn := d.DbHandle.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d.Delimiter)

	R := false
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		R = true
	}
	return R
}

// ExistTableRecord detect table has id record
func (d *Badger) ExistTableRecord(tableName, id string) bool {
	txn := d.DbHandle.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d.Delimiter + id + d.Delimiter)

	R := false
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		R = true
	}
	return R
}

// GetTable get a table's all values
func (d *Badger) GetTable(tableName, id string) map[string][]byte {

	txn := d.DbHandle.NewTransaction(false) // 新建只读事务
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d.Delimiter + id + d.Delimiter)
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
func (d *Badger) GetTableValue(tableName, id, field string) []byte {
	txn := d.DbHandle.NewTransaction(false)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer txn.Discard()
	defer it.Close()

	prefix := []byte(tableName + d.Delimiter + id + d.Delimiter + field)
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
func (d *Badger) EqualTableValue(tableName, id, field string, judgeValue []byte) bool {
	if bytes.Equal(judgeValue, d.GetTableValue(tableName, id, field)) {
		return true
	}
	return false
}
