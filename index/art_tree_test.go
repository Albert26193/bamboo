package index

import (
	"testing"
	"tiny-bitcask/content"

	"github.com/stretchr/testify/assert"
)

func TestPut(t *testing.T) {
	art := NewAdaptiveRadixTree()
	art.Put([]byte("key-a"), &content.LogStructIndex{FileIndex: 1, Offset: 18})
	art.Put([]byte("key-b"), &content.LogStructIndex{FileIndex: 1, Offset: 18})
	art.Put([]byte("key-c"), &content.LogStructIndex{FileIndex: 1, Offset: 18})

	assert.Equal(t, 3, art.Size())
}

func TestGet(t *testing.T) {
	art := NewAdaptiveRadixTree()
	art.Put([]byte("key-a"), &content.LogStructIndex{FileIndex: 1, Offset: 12})
	pos := art.Get([]byte("key-a"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-b"), &content.LogStructIndex{FileIndex: 123, Offset: 888})
	pos2 := art.Get([]byte("key-b"))
	assert.NotNil(t, pos2)
}

func TestDelete(t *testing.T) {
	art := NewAdaptiveRadixTree()

	res1, ok1 := art.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	art.Put([]byte("key-a"), &content.LogStructIndex{FileIndex: 1, Offset: 20})
	res2, ok2 := art.Delete([]byte("key-a"))
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res2.FileIndex)
	assert.Equal(t, int64(20), res2.Offset)

	pos := art.Get([]byte("key-a"))
	assert.Nil(t, pos)
}

func TestSize(t *testing.T) {
	art := NewAdaptiveRadixTree()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-a"), &content.LogStructIndex{FileIndex: 1, Offset: 2})
	art.Put([]byte("key-b"), &content.LogStructIndex{FileIndex: 1, Offset: 2})
	art.Put([]byte("key-c"), &content.LogStructIndex{FileIndex: 1, Offset: 2})
	assert.Equal(t, 3, art.Size())
}

func TestIterator(t *testing.T) {
	art := NewAdaptiveRadixTree()

	art.Put([]byte("abc"), &content.LogStructIndex{FileIndex: 1, Offset: 12})
	art.Put([]byte("cde"), &content.LogStructIndex{FileIndex: 1, Offset: 12})
	art.Put([]byte("fgh"), &content.LogStructIndex{FileIndex: 1, Offset: 12})
	art.Put([]byte("ijk"), &content.LogStructIndex{FileIndex: 1, Offset: 12})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
