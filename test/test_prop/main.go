package main

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"

	"github.com/lysShub/kvdb"
)

var err error
var pp map[string]map[string][]byte = map[string]map[string][]byte{
	"id1": {
		"field1": []byte("1111111111"),
		"field2": []byte("222222222222"),
	},
	"id2": {
		"field1": []byte("aaaaaaaaaaaaa"),
		"field2": []byte("bbbbbbbbbbbbbb"),
	},
	"id3": {
		"field1": []byte("aaaaaaaaaaaaaaa"),
		"field2": []byte("@@@@@@@@@@@@@@@@"),
	},
}
var id1p = map[string][]byte{
	"field1": []byte("1111111111"),
	"field2": []byte("222222222222"),
}

func Comprehensive(db *kvdb.KVDB) error {

	// 键值
	if err = db.SetKey("a1", []byte("a1")); err != nil {
		return err
	}
	if string(db.ReadKey("a1")) != "a1" {
		return errors.New("ReadKey1")
	}
	if err = db.DeleteKey("a1"); err != nil {
		return err
	}
	if string(db.ReadKey("a1")) != "" {
		return errors.New("ReadKey2")
	}

	// table
	if err = db.SetTable("test", pp); err != nil {
		return err
	}
	if !reflect.DeepEqual(db.ReadTable("test"), pp) {
		return errors.New("ReadTable")
	}
	if err = db.SetTableRow("test", "id2", id1p); err != nil {
		return err
	}
	if !reflect.DeepEqual(id1p, db.ReadTableRow("test", "id2")) {
		return errors.New("ReadTableRow")
	}
	if err = db.SetTableValue("test", "id2", "field2", []byte("19986")); err != nil {
		return err
	}

	if !bytes.Equal(db.ReadTableValue("test", "id2", "field2"), []byte("19986")) {
		return errors.New("ReadTableValue")
	}

	if r := db.ReadTableLimits("test", "field2", "=", 19986); len(r) == 1 && r[0] != "id2" {
		return errors.New("ReadTableLimits")
	}

	//
	if err = db.DeleteTable("test"); err != nil {
		return err
	}
	if db.ReadTableExist("test") != false {
		return errors.New("ReadTableExist")
	}
	return nil
}

func Write(db *kvdb.KVDB, i int) error {

	return db.SetTable("tableName"+strconv.Itoa(i), pp)
}
