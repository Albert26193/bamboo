package index

import (
	"bytes"
	"sort"
	"sync"
	"tiny-bitcask/content"

	artTree "github.com/plar/go-adaptive-radix-tree"
)

// ######################## indexer ########################
type AdaptiveRadixTree struct {
	tree     artTree.Tree
	treeLock *sync.RWMutex
}

func NewAdaptiveRadixTree() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree:     artTree.New(),
		treeLock: new(sync.RWMutex),
	}
}

func (a *AdaptiveRadixTree) Get(key []byte) *content.LogStructIndex {
	a.treeLock.RLock()
	defer a.treeLock.RUnlock()

	if value, ok := a.tree.Search(key); ok {
		return value.(*content.LogStructIndex)
	}
	return nil
}

// delete: return the old indexer, so we can calculate the size of used space
func (a *AdaptiveRadixTree) Delete(key []byte) (*content.LogStructIndex, bool) {
	a.treeLock.Lock()
	defer a.treeLock.Unlock()

	oldIndexer, hasDeleted := a.tree.Delete(key)

	if oldIndexer == nil {
		return nil, hasDeleted
	}

	return oldIndexer.(*content.LogStructIndex), hasDeleted
}

func (a *AdaptiveRadixTree) Size() int {
	a.treeLock.Lock()
	defer a.treeLock.Unlock()

	return a.tree.Size()
}

// put: return the old indexer, so we can calculate the size of used space
func (a *AdaptiveRadixTree) Put(key []byte, position *content.LogStructIndex) *content.LogStructIndex {
	a.treeLock.Lock()
	defer a.treeLock.Unlock()

	oldIndexer, _ := a.tree.Insert(key, position)
	if oldIndexer != nil {
		return oldIndexer.(*content.LogStructIndex)
	}
	return nil
}

func (a *AdaptiveRadixTree) Destroy() error {
	return nil
}

// ######################## Iterator ########################

type artTreeIterator struct {
	indexNumber    int
	isReverse      bool
	positionValues []*Entry
}

func NewArtTreeIterator(a artTree.Tree, reverse bool) *artTreeIterator {
	var curIndex = 0

	if reverse {
		curIndex = a.Size() - 1
	}

	posRecorder := make([]*Entry, a.Size())

	visitor := func(node artTree.Node) bool {
		e := &Entry{
			Key:      node.Key(),
			Position: node.Value().(*content.LogStructIndex),
		}

		posRecorder[curIndex] = e

		if reverse {
			curIndex--
		} else {
			curIndex++
		}

		return true
	}

	a.ForEach(visitor)

	return &artTreeIterator{
		indexNumber:    0,
		positionValues: posRecorder,
		isReverse:      reverse,
	}
}

// implement Iterator interface
func (ai *artTreeIterator) Next() {
	ai.indexNumber++
}

func (ai *artTreeIterator) Rewind() {
	ai.indexNumber = 0
}

func (ai *artTreeIterator) Value() *content.LogStructIndex {
	return ai.positionValues[ai.indexNumber].Position
}

func (ai *artTreeIterator) Key() []byte {
	return ai.positionValues[ai.indexNumber].Key
}

func (ai *artTreeIterator) Seek(key []byte) {
	if ai.isReverse {
		ai.indexNumber = sort.Search(len(ai.positionValues), func(i int) bool {
			return bytes.Compare(ai.positionValues[i].Key, key) <= 0
		})
	} else {
		ai.indexNumber = sort.Search(len(ai.positionValues), func(i int) bool {
			return bytes.Compare(ai.positionValues[i].Key, key) >= 0
		})
	}
}

func (ai *artTreeIterator) Valid() bool {
	return ai.indexNumber < len(ai.positionValues)
}

func (ai *artTreeIterator) Close() {
	ai.positionValues = nil
}

// implement Iterator
func (a *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	a.treeLock.Lock()
	defer a.treeLock.Unlock()
	return NewArtTreeIterator(a.tree, reverse)
}
