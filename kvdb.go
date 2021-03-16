package kvdb

import (
	"errors"
	"kvdb/badger"
	"kvdb/boltdb"
	"time"
)

// Handle 同一数据库句柄
type Handle struct {
	badger badger.Handle
	boltdb boltdb.Boltdb
}

// key/value database
// 有简单的key/value键值对储存；badger是没有前缀存储，boltdb是存储在特定的一个桶中
// 也有表结构存储；badger是使用前缀实现的，boltdb是使用桶嵌套实现的
// 在表中，每一条记录都有一此表中唯一id将各项字段联系起来，类似主键
type KVDB struct {
	Type uint8  // 必须 0:badgerdb; 1:boltdb
	DH   Handle // 数据库句柄
	//
	Path     string   // 数据库存储路径
	Password [16]byte // 密码，仅适用于badgerdb
	RAMMode  bool     // 内存模式，仅适用于badgerdb
}

// 将操作与预期不符将返回错误；如删除表中某一条记录，但此表不存在，将不会有错误信息返回
// 所有字段名使用string，所有值使用[]byte

var errType error = errors.New("invalid value of KVDB.Type")

// 初始化函数
func (d *KVDB) Init() error {
	if d.Type == 0 { //badgerdb
		var badgerdb = new(badger.Badger)

		badgerdb.Path = d.Path
		badgerdb.Password = d.Password
		badgerdb.RAM = d.RAMMode
		badgerdb.Delimiter = "`"
		if err := badgerdb.OpenDb(); err != nil {
			return err
		}
		d.DH.badger = badgerdb.DbHandle

	} else if d.Type == 1 { //blotdb

	}
	return errType
}

func (d *KVDB) Close() {
	if d.Type == 0 { //badgerdb
		d.DH.badger.Close()
	} else if d.Type == 1 { //blotdb
		d.DH.boltdb.Close()
	}
	return
}

// key/value 操作

// SetKey 设置/修改一个值
func (d *KVDB) SetKey(key string, value []byte, ttl ...time.Duration) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.SetKey(key, value)
	}
	return errType
}

// DeleteKey 删除一个值
func (d *KVDB) DeleteKey(key string) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.DeleteKey(key)
	}
	return errType
}

// ReadKey 读取一个值
func (d *KVDB) ReadKey(key string) []byte {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.ReadKey(key)
	}
	return nil
}

// 表操作

// SetTable 设置/修改一个表
func (d *KVDB) SetTable(tableName string, p map[string]map[string][]byte, ttl ...time.Duration) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.SetTable(tableName, p)
	}
	return errType
}

// SetTableRow 设置/修改一个表中的一条记录
func (d *KVDB) SetTableRow(tableName, id string, p map[string][]byte, ttl ...time.Duration) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.SetTableRow(tableName, id, p)
	}
	return errType
}

// SetTableValue 设置/修改一个表中的一条记录的某个字段的值
func (d *KVDB) SetTableValue(tableName, id, field string, value []byte, ttl ...time.Duration) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.SetTableValue(tableName, id, field, value)
	}
	return errType
}

// DeleteTable 删除一个表
func (d *KVDB) DeleteTable(tableName string) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.DeleteTable(tableName)
	}
	return errType
}

// DeleteTableRow 删除表中的某条记录
func (d *KVDB) DeleteTableRow(tableName, id string) error {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.DeleteTableRow(tableName, id)
	}
	return errType
}

// ReadTable 读取一个表中所有数据
func (d *KVDB) ReadTable(tableName string) map[string]map[string][]byte {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.ReadTable(tableName)
	}
	return nil
}

// ReadTableExist 表是否存在
func (d *KVDB) ReadTableExist(tableName string) bool {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.ReadTableExist(tableName)
	}
	return false
}

// ReadTableRow 读取表中一条记录
func (d *KVDB) ReadTableRow(tableName, id string) map[string][]byte {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.ReadTableRow(tableName, id)
	}
	return nil
}

// ReadTableValue 读取表中一条记录的某个字段值
func (d *KVDB) ReadTableValue(tableName, id, field string) []byte {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.ReadTableValue(tableName, id, field)
	}
	return nil
}

// ReadTableLimits 获取表中满足条件的所有id
func (d *KVDB) ReadTableLimits(tableName, field, exp string, value int) []string {
	if d.Type == 0 {

	} else if d.Type == 1 {
		return d.DH.boltdb.ReadTableLimits(tableName, field, exp, value)
	}
	return nil
}
