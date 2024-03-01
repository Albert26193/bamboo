package index

import (
	"bamboo/content"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTreePut(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &content.LogStructIndex{FileIndex: 1, Offset: 100})
	assert.Nil(t, res1)

	res2 := bt.Put([]byte("a"), &content.LogStructIndex{FileIndex: 1, Offset: 2})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &content.LogStructIndex{FileIndex: 11, Offset: 12})
	assert.Equal(t, res3.FileIndex, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

func TestBTreeGet(t *testing.T) {
	bt := NewBtree()

	res1 := bt.Put(nil, &content.LogStructIndex{FileIndex: 1, Offset: 100})
	assert.Nil(t, res1)

	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.FileIndex)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &content.LogStructIndex{FileIndex: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &content.LogStructIndex{FileIndex: 1, Offset: 3})
	assert.Equal(t, res3.FileIndex, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.FileIndex)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBtreeDelete(t *testing.T) {
	bt := NewBtree()
	res1 := bt.Put(nil, &content.LogStructIndex{FileIndex: 1, Offset: 100})
	assert.Nil(t, res1)
	res2, ok1 := bt.Delete(nil)
	assert.True(t, ok1)
	assert.Equal(t, res2.FileIndex, uint32(1))
	assert.Equal(t, res2.Offset, int64(100))

	res3 := bt.Put([]byte("aaa"), &content.LogStructIndex{FileIndex: 22, Offset: 33})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("aaa"))
	assert.True(t, ok2)
	assert.Equal(t, res4.FileIndex, uint32(22))
	assert.Equal(t, res4.Offset, int64(33))

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
