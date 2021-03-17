package main

import (
	"fmt"
	"kvdb"
	"time"
)

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

func main() {

	var err error
	var db = new(kvdb.KVDB)
	defer db.Close()
	db.Type = 0
	if err = db.Init(); err != nil {
		fmt.Println(0, err)
		return
	}
	a := time.Now().UnixNano()

	if err = db.SetTable("test", pp); err != nil {
		fmt.Println(1, err)
		return
	}

	fmt.Println(db.ReadTable("test"))
	fmt.Println((time.Now().UnixNano() - a) / 1e6)

}
