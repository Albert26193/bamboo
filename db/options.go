package db

import "os"

type Options struct {
	DataDir   string
	DataSize  uint32
	SyncData  bool
	IndexType IndexType
}

type IteratorOptions struct {
	Prefix  []byte
	Reverse bool
}

type IndexType = int8

const (
	BTree IndexType = 0
	ART   IndexType = 1
)

var DefaultOptions = Options{
	DataDir:   os.TempDir(),
	DataSize:  256 * 1024 * 1024,
	SyncData:  false,
	IndexType: BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteOptions struct {
	MaxWriteCount uint
	SyncCommit    bool
}

var DefaultWriteOptions = WriteOptions{
	MaxWriteCount: 10000,
	SyncCommit:    false,
}
