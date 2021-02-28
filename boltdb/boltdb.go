package boltdb

import (
	"bytes"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

/*
* CURD本地数据
* 使用 bolt:github.com/boltdb/bolt
* 所有value为[]byte； tableName和key为string
* 所有的写入操作，不存在将新建；所有的删除操作，不存在将不报错；所有的读取操作，不存在将返回空字符串
* bolt 使用于客户端(体积小)
 */

// Handle handle
type Handle = *bolt.DB

// special bucket for original key/value store,
// all base operation is in this bucket
const sn = "_root"

var err error

// OpenDb open
func OpenDb(path string) (Handle, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// CloseDb close
func CloseDb(h Handle) error {
	return h.Close()
}

/*
* base operation( original key/value )
* all key/value store in special bucket(_root)
 */

// SetKey set or updata key/value
func SetKey(key string, value []byte, h Handle) error {
	err = h.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sn))
		if b == nil {
			_, err := tx.CreateBucket([]byte(sn))
			if err != nil {
				return err
			}
		}
		c := tx.Bucket([]byte(sn))
		return c.Put([]byte(key), value)
	})
	return err
}

// DeleteKey delete key
func DeleteKey(key string, h Handle) error {
	err = h.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sn))
		if b == nil {
			_, err := tx.CreateBucket([]byte(sn))
			if err != nil {
				return err
			}
		}
		c := tx.Bucket([]byte(sn))
		return c.Delete([]byte(key))
	})
	return nil
}

// GetValue get value
func GetValue(key string, h Handle) []byte {
	var r []byte
	err = h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sn))
		if b == nil {
			var err error = errors.New("不存在表：" + sn)
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if bytes.Equal(k, []byte(key)) {
				r = v
				break
			}
		}
		return nil
	})
	return r
}

/*
* Table operation, by bucket nested achieve
* get a value need parameters: tableName, field, id;
* bucket name = tableName
* secondary bucket name = id(id as PRIMARY_KEY in sql)
 */

// CreatTable create/set a table; exist will be rewrite
func CreatTable(tableName, id string, fv map[string][]byte, h Handle) error {
	err = h.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}

		sb, err := b.CreateBucketIfNotExists([]byte(id)) //secondary bucket
		if err != nil {
			return err
		}

		for f, v := range fv {
			err := sb.Put([]byte(f), v)
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// SetTableValue set/updata a table's value
func SetTableValue(tableName, id, field, string, value []byte, h Handle) error {
	err = h.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}

		sb, err := b.CreateBucketIfNotExists([]byte(id)) //secondary bucket
		if sb == nil {
			return err
		}

		err = sb.Put([]byte(field), value)
		return err
	})
	return err
}

// ExistTable detect tableName is exist
func ExistTable(tableName string, h Handle) bool {
	var r bool
	_ = h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b != nil {
			r = true
		} else {
			r = false
		}
		return nil
	})
	return r
}

// ExistTableRecord detect table has id record
func ExistTableRecord(tableName, id string, h Handle) bool {
	var r bool
	_ = h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			r = false
			return nil
		}
		sb := b.Bucket([]byte(id))
		if sb == nil {
			r = false
		} else {
			r = true
		}
		return nil
	})
	return r
}

// GetTable get a table's all values
func GetTable(tableName, id string, h Handle) map[string][]byte {
	var r map[string][]byte = make(map[string][]byte)
	_ = h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			r = nil
			return nil
		}
		sb := b.Bucket([]byte(id))
		if sb == nil {
			r = nil
			return nil
		}
		c := sb.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			r[string(k)] = v
		}
		return nil
	})
	return r
}

// GetTableValue get one specify value
func GetTableValue(tableName, id, field string, h Handle) []byte {
	var r []byte
	_ = h.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			r = nil
			return nil
		}
		sb := b.Bucket([]byte(id))
		if sb == nil {
			r = nil
			return nil
		}
		r = sb.Get([]byte(field))
		return nil
	})
	return r
}

// EqualTableValue check the specify value is equal judgeValue
func EqualTableValue(tableName, id, field string, judgeValue []byte, h Handle) bool {
	if bytes.Equal(judgeValue, GetTableValue(tableName, id, field, h)) {
		return true
	}
	return false
}
