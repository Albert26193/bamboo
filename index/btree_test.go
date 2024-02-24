package index

import (
	"testing"
	"tiny-bitcask/content"

	"github.com/stretchr/testify/assert"
)

func TestBtreePut(t *testing.T) {
	b := NewBtree()
	key := []byte("key")
	position := &content.LogStructIndex{
		FileIndex: 1,
		Offset:    2,
	}
	got := b.Put(key, position)
	assert.True(t, got)
}

func TestBtreeGet(t *testing.T) {
	b := NewBtree()
	key := []byte("key")
	position := &content.LogStructIndex{
		FileIndex: 1,
		Offset:    2,
	}
	b.Put(key, position)
	got := b.Get(key)
	assert.Equal(t, position, got)
}

func TestBtreeDelete(t *testing.T) {
	b := NewBtree()
	key := []byte("key")
	position := &content.LogStructIndex{
		FileIndex: 1,
		Offset:    2,
	}
	b.Put(key, position)
	got := b.Delete(key)
	assert.True(t, got)
}

func TestBTreeIterator(t *testing.T) {
	bt1 := NewBtree()

	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	bt1.Put([]byte("ccde"), &content.LogStructIndex{FileIndex: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	bt1.Put([]byte("acee"), &content.LogStructIndex{FileIndex: 1, Offset: 10})
	bt1.Put([]byte("eede"), &content.LogStructIndex{FileIndex: 1, Offset: 10})
	bt1.Put([]byte("bbcd"), &content.LogStructIndex{FileIndex: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
