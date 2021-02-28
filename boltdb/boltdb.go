package boltdb

import (
	"errors"
	"os"
	"path/filepath"
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

//获取二进制文件储存路径
func dbBindaryPath() string {
	return filepath.ToSlash(filepath.Dir(os.Args[0])) + `/database.db`
}

// CreateTable 创建表(不存在将创建；存在将追加，key重名将覆盖)
// 表名 数据表
func CreateTable(tableName string, mapList map[string][]byte) error {
	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))

		if b == nil { // 不存在表，创建
			_, err := tx.CreateBucket([]byte(tableName))
			if err != nil {
				return err
			}
		}
		// 写入
		for key, v := range mapList {
			c := tx.Bucket([]byte(tableName))
			err := c.Put([]byte(key), v)
			if err != nil {
				return err
			}
		}
		return nil

	})
	return err
}

// ExistTable 表是否存在
func ExistTable(tableName string) bool {
	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return false
	}
	defer db.Close()

	err1 := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return nil // 不存在
		}
		return errors.New("存在")
	})
	if err1 == nil {
		return false
	}
	return true
}

//GetTable 获取表所有数据
func GetTable(tableName string) (map[string][]byte, error) {
	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	defer db.Close()
	R := make(map[string][]byte)

	err1 := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			var err error = errors.New("不存在表：" + tableName)
			return err
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if k != nil && v == nil {
				continue
			}
			R[string(k)] = v
		}

		return nil
	})

	return R, err1
}

// GetTableValue 读取tableName表中key的值
// 字段名 表名(空默认为root表)
func GetTableValue(key string, tableName string) (string, error) {

	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return "", err
	}
	defer db.Close()

	var val string
	err = db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b != nil {
			data := b.Get([]byte(key))

			val = string(data)
			if val == "" {
				var err1 error = errors.New("表：" + tableName + "中,值：" + key + "不存在")
				return err1
			}
		} else {
			var err1 error = errors.New("表：" + tableName + "不存在")
			return err1
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return val, nil
}

// SetTableValue 在表中增改一个key:value; 存在将会覆盖;表不存在将创建
// 字段名 值 表名
func SetTableValue(key string, value []byte, tableName string) error {

	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {

		b := tx.Bucket([]byte(tableName))

		if b == nil { // 不存在tn表，创建
			_, err := tx.CreateBucket([]byte(tableName))
			if err != nil {
				return err
			}
		}

		//现在表一定存在 写入
		c := tx.Bucket([]byte(tableName))
		err := c.Put([]byte(key), value)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

// DeleteTableKey 删除一个key
func DeleteTableKey(tableName, key string) error {
	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(tableName)).Delete([]byte(key))
	})
	return err
}

// DeleteTable 删除一个表
func DeleteTable(tableName string) error {
	db, err := bolt.Open(dbBindaryPath(), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()

	//不存在不返回错误
	isExist := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tableName))
		if b == nil {
			return nil
		}
		return errors.New("表不存在")
	})
	if isExist == nil {
		return nil
	}

	err = db.Update(func(tx *bolt.Tx) error {
		c := tx.DeleteBucket([]byte(tableName))
		return c
	})
	return err
}
