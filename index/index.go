package index

import (
	"bytes"
	"tiny-bitcask/content"

	"github.com/google/btree"
)

type IndexType = int8

const (
	BtreeIndex IndexType = 0
	ART        IndexType = 1
)

type Indexer interface {
	// Put content into the index
	Put(key []byte, position *content.LogStructIndex) bool
	// Get returns the index of the file
	Get(key []byte) *content.LogStructIndex
	// Delete removes the index of the file
	Delete(key []byte) bool
	// get Size
	Size() int
	// Iterator returns an iterator for the index
	Iterator(reverse bool) Iterator
}

type Entry struct {
	Key      []byte
	Position *content.LogStructIndex
}

func (e *Entry) Less(bi btree.Item) bool {
	return bytes.Compare(e.Key, bi.(*Entry).Key) < 0
}

func NewIndexer(indexType IndexType) Indexer {
	switch indexType {
	case BtreeIndex:
		return NewBtree()
	case ART:
		return nil
	default:
		panic("Unknown index type")
	}
}

type Iterator interface {
	// Next: move the iterator to the next key
	Next()

	// rewind: move the iterator to the first key
	Rewind()

	// Seek: move the iterator to the first key that is greater or equal to the given key
	Seek(key []byte)

	// Valid: return true if the iterator is valid
	Valid() bool

	// Key: return the key of the current iterator
	Key() []byte

	// Value: return the value of the current iterator
	Value() *content.LogStructIndex

	// Close
	Close()

}
