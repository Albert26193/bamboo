package index

import (
	"bytes"
	"tiny-bitcask/content"

	"github.com/google/btree"
)

type Indexer interface {
	// Put content into the index
	Put(key []byte, position *content.LogStructIndex) bool

	// Get returns the index of the file
	Get(key []byte) *content.LogStructIndex

	// Delete removes the index of the file
	Delete(key []byte) bool
}

type Entry struct {
	Key      []byte
	Position *content.LogStructIndex
}

func (e *Entry) Less(bi btree.Item) bool {
	return bytes.Compare(e.Key, bi.(*Entry).Key) < 0
}
