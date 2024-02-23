package db

type Options struct {
	DataDir   string
	DataSize  uint32
	SyncData  bool
	IndexType IndexType
}

type IndexType = int8

const (
	BTree IndexType = 1
	ART IndexType = 2
)
