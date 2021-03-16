package main

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
)

func main() {

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

	db, err := bolt.Open(`D:\OneDrive\code\go\project\kvdb\test\badger_test.db`, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	if err := SetTable(db, "艹", pp); err != nil {
		fmt.Println(err)
		return
	}

	var buk *bolt.Bucket
	db.Update(func(tx *bolt.Tx) error {
		buk = tx.Bucket([]byte("艹"))
		if buk == nil {
			fmt.Println("打开出错")
		}
		displayBolt2(db, buk, 2)
		return nil
	})

}

func SetTable(d *bolt.DB, tableName string, p map[string]map[string][]byte) error {

	err := d.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(tableName))
		if err != nil {
			return err
		}

		var sb *bolt.Bucket
		for id, fv := range p {
			if sb, err = b.CreateBucketIfNotExists([]byte(id)); err != nil {
				return err
			}
			for f, v := range fv {
				if err = sb.Put([]byte(f), v); err != nil {
					return err
				}
			}
		}

		return nil
	})
	return err
}

func displayBolt2(db *bolt.DB, buk *bolt.Bucket, pos int) error {
	// buk *bolt.Bucket

	err := db.View(func(tx *bolt.Tx) error {
		var c *bolt.Cursor
		if buk == nil {
			c = tx.Cursor()
			fmt.Println("ROOT")
		} else {
			c = buk.Cursor()
		}
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if k != nil && v == nil {
				fmt.Printf("ID：%s\n", k) //bucket
				var buk2 *bolt.Bucket
				if buk == nil {
					buk2 = tx.Bucket(k)
				} else {
					buk2 = buk.Bucket(k)
				}
				displayBolt2(db, buk2, pos+2)
			}
			// if k == nil {
			// 	fmt.Println(" ----nil - never") //never will happend
			// } else {
			// 	for i := 0; i < pos; i++ {
			// 		fmt.Print(" ")
			// 	}
			// 	if v == nil {
			// 		fmt.Printf("ID：%s\n", k) //bucket
			// 		var buk2 *bolt.Bucket
			// 		if buk == nil {
			// 			buk2 = tx.Bucket(k)
			// 		} else {
			// 			buk2 = buk.Bucket(k)
			// 		}
			// 		displayBolt2(db, buk2, pos+2)
			// 	} else {
			// 		fmt.Printf("%s=%s\n", k, v) // k = v
			// 	}
			// }
		}
		return nil
	})
	return err
}
