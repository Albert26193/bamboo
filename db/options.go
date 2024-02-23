package db

type Options struct {
	DataDir   string
	DataSize  uint32
	SyncData  bool
	IndexType IndexType
}

type IndexType = int8

const (
	BTree IndexType = iota + 1
	ART
)
