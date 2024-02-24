package db

import (
	"tiny-bitcask/index"
)

// Iterator is an interface for iterating over key-value pairs in a Bitcask database.
type Iterator struct {
	indexIterator index.Iterator
	db            *DB
	options       IteratorOptions
}

// NewIterator creates a new Iterator.
func (db *DB) NewIterator(options IteratorOptions) *Iterator {
	indexIterator := db.index.Iterator(options.Reverse)
	return &Iterator{
		indexIterator: indexIterator,
		db:            db,
		options:       options,
	}
}

// Next moves the iterator to the next key-value pair.
func (i *Iterator) Next() {
	i.indexIterator.Next()
}

// Valid returns true if the iterator is positioned at a valid key-value pair.
func (i *Iterator) Valid() bool {
	return i.indexIterator.Valid()
}

// Skip to next key
func (i *Iterator) Skip() {
	prefixLen := len(i.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for i.Valid() {
		key := i.indexIterator.Key()
		if len(key) >= prefixLen && string(key[:prefixLen]) == string(i.options.Prefix) {
			return
		}
		i.Next()
	}
}

// Seek: find the first key that is greater or equal to the given key.
func (i *Iterator) Seek(key []byte) {
	i.indexIterator.Seek(key)
	i.Skip()
}

// Rewind moves the iterator to the first key-value pair.
func (i *Iterator) Rewind() {
	i.indexIterator.Rewind()
	i.Skip()
}

// Key returns the key of the current key-value pair.
func (i *Iterator) Key() []byte {
	return i.indexIterator.Key()
}

// Value returns the value of the current key-value pair.
func (i *Iterator) Value() ([]byte, error) {
	logPos := i.indexIterator.Value()
	i.db.muLock.RLock()

	defer i.db.muLock.RUnlock()
	return i.db.GetValueFormPos(logPos)
}

// Close iterator, release resources.
func (i *Iterator) Close() {
	i.indexIterator.Close()
}
