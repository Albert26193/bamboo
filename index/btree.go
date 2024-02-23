package index

import (
	"sync"
	"tiny-bitcask/content"

	"github.com/google/btree"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (b *Btree) Put(key []byte, position *content.LogStructIndex) bool {
	e := &Entry{Key: key, Position: position}
	b.lock.Lock()
	b.tree.ReplaceOrInsert(e)
	b.lock.Unlock()
	return true
}

// google btree read is safe, no need to lock
func (b *Btree) Get(key []byte) *content.LogStructIndex {
	e := &Entry{Key: key}
	item := b.tree.Get(e)
	if item == nil {
		return nil
	}
	return item.(*Entry).Position
}

func (b *Btree) Delete(key []byte) bool {
	e := &Entry{Key: key}
	b.lock.Lock()
	removeItem := b.tree.Delete(e)
	b.lock.Unlock()

	return removeItem != nil
}
