package boltdb

import (
	"kvdb/com"
	"time"

	"github.com/boltdb/bolt"
)

// Handle handle
type Handle = *bolt.DB

type Boltdb struct {
	Path     string
	DbHandle Handle
	Root     string
}

// 单纯key/value储存桶
const sn = "_root"

var err error
var b *bolt.Bucket

// OpenDb open
func (d *Boltdb) OpenDb(path string) error {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	d.DbHandle = db
	return nil
}

// CloseDb close
func (d *Boltdb) Close() error {
	return d.DbHandle.Close()
}

// key/value

// SetKey set or updata key/value
func (d *Boltdb) SetKey(key string, value []byte) error {
	err = d.DbHandle.Update(func(tx *bolt.Tx) error {
		if b, err = tx.CreateBucketIfNotExists([]byte(sn)); err != nil {
			return err
		}
		return b.Put([]byte(key), value)
	})
	return err
}

// DeleteKey delete key
func (d *Boltdb) DeleteKey(key string) error {
	err = d.DbHandle.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sn))
		if b == nil {
			return nil
		}
		return b.Delete([]byte(key))
	})
	return nil
}

// ReadKey
func (d *Boltdb) ReadKey(key string) []byte {
	var r []byte
	err = d.DbHandle.View(func(tx *bolt.Tx) error {
		if b = tx.Bucket([]byte(sn)); b == nil {
			return nil
		}
		_, r = b.Cursor().Seek([]byte(key))
		return nil
	})
	return r
}

// table

// SetTable
func (d *Boltdb) SetTable(tableName string, p map[string]map[string][]byte) error {

	err = d.DbHandle.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}

		var sb *bolt.Bucket
		for id, fv := range p {
			if sb, err = b.CreateBucketIfNotExists([]byte(id)); err != nil { //sb: secondary bucket
				return err
			}
			for f, v := range fv {
				err := sb.Put([]byte(f), v)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
	return err
}

// SetTableRow
func (d *Boltdb) SetTableRow(tableName, id string, fv map[string][]byte) error {
	err = d.DbHandle.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}

		var sb *bolt.Bucket
		if sb, err = b.CreateBucketIfNotExists([]byte(id)); err != nil {
			return err
		}

		for f, v := range fv {
			if err = sb.Put([]byte(f), v); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// SetTableValue
func (d *Boltdb) SetTableValue(tableName, id, field string, value []byte) error {
	err = d.DbHandle.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}

		var sb *bolt.Bucket
		if sb, err := b.CreateBucketIfNotExists([]byte(id)); sb == nil {
			return err
		}

		return sb.Put([]byte(field), value)
	})
	return err
}

// DeleteTable
func (d *Boltdb) DeleteTable(tableName string) error {
	err = d.DbHandle.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket([]byte(tableName))
	})
	return err
}

// DeleteTableRow
func (d *Boltdb) DeleteTableRow(tableName, id string) error {
	err = d.DbHandle.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil { // bucket not exist
			return nil
		}
		return b.DeleteBucket([]byte(id))
	})
	return err
}

// ReadTable
func (d *Boltdb) ReadTable(tableName string) map[string]map[string][]byte {
	var r map[string]map[string][]byte = make(map[string]map[string][]byte)

	_ = d.DbHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return nil
		}

		var c, sc *bolt.Cursor = b.Cursor(), nil
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if k != nil && v == nil {
				var tmp map[string][]byte = make(map[string][]byte)
				sc = tx.Bucket(k).Cursor()
				for k, v := sc.First(); k != nil; k, v = sc.Next() {
					if k != nil && v != nil {
						tmp[string(k)] = v
					}
				}
				r[string(k)] = tmp
			}
		}
		return nil
	})
	return r
}

// ReadTableExist
func (d *Boltdb) ReadTableExist(tableName string) bool {
	var r bool
	_ = d.DbHandle.View(func(tx *bolt.Tx) error {
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

// ReadTableRow
func (d *Boltdb) ReadTableRow(tableName, id string) map[string][]byte {
	var r map[string][]byte = make(map[string][]byte)
	_ = d.DbHandle.View(func(tx *bolt.Tx) error {
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

// ReadTableValue
func (d *Boltdb) ReadTableValue(tableName, id, field string) []byte {
	var r []byte
	_ = d.DbHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return nil
		}
		var sb *bolt.Bucket
		if sb = b.Bucket([]byte(id)); sb == nil {
			return nil
		}
		r = sb.Get([]byte(field))
		return nil
	})
	return r
}

// ReadTableLimits
func (d *Boltdb) ReadTableLimits(tableName, field, exp string, value int) []string {
	var r []string
	_ = d.DbHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			r = nil
			return nil
		}

		var c, sc *bolt.Cursor = b.Cursor(), nil
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if k != nil && v == nil {
				id := k

				sc = tx.Bucket(k).Cursor()
				for k, v := sc.First(); k != nil; k, v = sc.Next() {
					if string(k) == field {
						fag, err := com.ExpressionCalculate(exp, value, v)
						if err != nil {
							return err
						}
						if fag {
							r = append(r, string(id))
						}

					}
				}
			}
		}
		return nil
	})

	return r
}
