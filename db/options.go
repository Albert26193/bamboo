package db

import "os"

type Options struct {
	DataDir   string
	DataSize  uint32
	SyncData  bool
	IndexType IndexType
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
