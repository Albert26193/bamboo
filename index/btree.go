package index

import (
	"bytes"
	"sort"
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

func (b *Btree) Size() int {
	return b.tree.Len()
}

type btreeIterator struct {
	currentIndex int
	reverse      bool
	values       []*Entry
}

func NewBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var index int
	values := make([]*Entry, tree.Len())

	getList := func(it btree.Item) bool {
		values[index] = it.(*Entry)
		index++
		return true
	}

	if reverse {
		tree.Descend(getList)
	} else {
		tree.Ascend(getList)
	}

	return &btreeIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (bi *btreeIterator) Next() {
	bi.currentIndex++
}

func (bi *btreeIterator) Valid() bool {
	return bi.currentIndex < len(bi.values)
}

func (bi *btreeIterator) Seek(key []byte) {
	if bi.reverse {
		bi.currentIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].Key, key) <= 0
		})
	} else {
		bi.currentIndex = sort.Search(len(bi.values), func(i int) bool {
			return bytes.Compare(bi.values[i].Key, key) >= 0
		})
	}
}

func (bi *btreeIterator) Key() []byte {
	return bi.values[bi.currentIndex].Key
}

func (bi *btreeIterator) Value() *content.LogStructIndex {
	return bi.values[bi.currentIndex].Position
}

func (bi *btreeIterator) Rewind() {
	bi.currentIndex = 0
}

func (bi *btreeIterator) Close() {
	bi.values = nil
}

func (b *Btree) Iterator(reverse bool) Iterator {
	if b.tree == nil {
		return nil
	}
	b.lock.RLock()
	defer b.lock.RUnlock()
	return NewBtreeIterator(b.tree, reverse)
}

