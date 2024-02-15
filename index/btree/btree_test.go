package btree

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
