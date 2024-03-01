# Bamboo

> ❗ the project is still under development, and the current version is 0.0.0

## intro

- Bamboo is a simple, high-performance, and reliable open-source DB engine for **key-value storage**.
- Bamboo is written in Go, based on the **Bitcask** model, which supports [Riak-KV](https://riak.com/products/riak-kv/index.html)
- Bamboo symbolizes rapid unidirectional growth, which is similar to the characteristics of a **log-structured** database. 

## Quick Start

- Below is a simple example of using Bamboo to store data.

```go
package main

import (
	bamboo "bamboo/db"
	"fmt"
)

func main() {
    opts := bamboo.DefaultOptions
    opts.DataDir = "/tmp/bamboo-demo"
    db, err := bamboo.CreateDB(opts)
    if err != nil {
      panic(err)
    }

    err = db.Put([]byte("name"), []byte("bamboo"))
    if err != nil {
      panic(err)
    }
    val, err := db.Get([]byte("name"))
    if err != nil {
      panic(err)
    }
    fmt.Println("val = ", string(val))
}

```

## Features

- **Only One-Time Disk IO**: Whether it is `GET`, `PUT`, or `DELETE`, this storage engine has at most one disk IO operation, thus ensuring high speed and predictable latency. 
- **Multiple Indexer**: Users can use different indexers to store data, such as `art-tree` and `btree`.
- **Quick Rebuild**: Use `MMap` to quickly rebuild the index when the database is opened.

## Benchmark

- the benchmark is based on the `Btree` indexer, and the test environment is a `E5-2696v4` CPU, `512G` memory, and `10T` HDD.

| Operation | Time (ns/op) | Memory (B/op) |  Allocs (allocs/op) |
| --------- | --- | ------- |-----|
| Write     | 50423 | 4673 | 11 |
| Read      | 1231  | 72   | 3 |
| Delete    | 853   | 72   | 3 |


## Future

- **Support for Multiple Data Types**: In the future, Bamboo will support more data types, such as `list`, `set`, and `hash`.
- **More Benchmark**: In the future, Bamboo will support more benchmark tests, such as `YCSB`.
- **Disk IO Optimization**: In the future, Bamboo will support more disk IO optimization, such as `WAL`.

## Thanks

- Great thanks to the [RoseDB](https://github.com/rosedblabs/rosedb), and [miniDB](https://github.com/rosedblabs/minidb), which provide a lot of inspiration for this project.