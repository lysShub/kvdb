package main

import (
	"fmt"
	"kvdb"
	"testing"
)

func BenchmarkComprehensive_blot(b *testing.B) {
	var db = new(kvdb.KVDB)
	db.Type = 1
	if err = db.Init(); err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		if err = Comprehensive(db); err != nil {
			fmt.Println("BenchmarkComprehensive_blot", err)
		}
	}
}
func BenchmarkComprehensive_badger(b *testing.B) {
	var db = new(kvdb.KVDB)
	db.Type = 0
	if err = db.Init(); err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		if err = Comprehensive(db); err != nil {
			fmt.Println("BenchmarkComprehensive_blot", err)
		}
	}
}

func BenchmarkComprehensive_badgerRAM(b *testing.B) {
	var db = new(kvdb.KVDB)
	db.Type = 0
	db.RAMMode = true
	if err = db.Init(); err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		if err = Comprehensive(db); err != nil {
			fmt.Println("BenchmarkComprehensive_blot", err)
		}
	}
}
func BenchmarkWrite_blot(b *testing.B) {
	var db = new(kvdb.KVDB)
	db.Type = 1
	if err = db.Init(); err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		if err = Write(db, i); err != nil {
			fmt.Println("BenchmarkComprehensive_blot", err)
		}
	}
}
func BenchmarkWrite_badger(b *testing.B) {
	var db = new(kvdb.KVDB)
	db.Type = 0
	if err = db.Init(); err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		if err = Write(db, i); err != nil {
			fmt.Println("BenchmarkComprehensive_blot", err)
		}
	}
}
func BenchmarkWrite_badgerRAM(b *testing.B) {
	var db = new(kvdb.KVDB)
	db.Type = 0
	db.RAMMode = true
	if err = db.Init(); err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	for i := 0; i < b.N; i++ {
		if err = Write(db, i); err != nil {
			fmt.Println("BenchmarkComprehensive_blot", err)
		}
	}
}
