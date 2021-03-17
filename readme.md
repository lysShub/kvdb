# Golang key/Value database api

我是一个大裁缝！

Golang的key/value 储存可以在无SQL的情况下实现数据的存储，在部分场景下非常优雅，但是通常只有键值对形式的存储。在本仓库中，对api进行了统一，有单纯的键值对存储，还有间接实现的以表的结构的储存。本仓库集成了以下key/value数据库：

- [badgerdb](github.com/dgraph-io/badger/v2)
- [boltdb](github.com/boltdb/bolt)

badger通过前缀实现表的结构，boltdb通过bucket嵌套实现表的结构；无论怎样，它们都通过一个唯一ID将几个字段联系在一起。

###### Start

GO111MODULE=on

```shell
git clone https://github.com/lysShub/kvdb.git
cd ./kvdb/test
go build -o test test.go
,/test
```

###### 如何选择

- 性能测试

```shell
goos: windows
goarch: amd64
pkg: kvdb/test/test_prop
BenchmarkComprehensive_blot-8                 30          61566827 ns/op #blotdb综合读写
BenchmarkComprehensive_badger-8              656           4951209 ns/op #badgerdb综合读写
BenchmarkComprehensive_badgerRAM-8          1480           4679729 ns/op #blotdb内存模式综合读写
BenchmarkWrite_blot-8                        241           5373580 ns/op #blotdb写
BenchmarkWrite_badger-8                    30897             37654 ns/op #badgerdb写
BenchmarkWrite_badgerRAM-8                 34824             47725 ns/op #badgerdb内存模式写
PASS
ok      kvdb/test/test_prop     19.514s
```

badgerdb综合性能是boltdb的十余倍，而且写是百余辈

- 功能

badgerdb的功能比boltdb更多，比如可以加密，可以有高性能的内存模式，还可以设置TTL

- 其他

boltdb是一单个文件形式存储、更友好，badgerdb需要一个文件夹