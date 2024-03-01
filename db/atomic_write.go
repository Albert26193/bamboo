package db

import (
	"bamboo/content"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

type atomicWrite struct {
	db          *DB
	dataToWrite map[string]*content.LogStruct
	muLock      *sync.RWMutex
	options     WriteOptions
}

var finishedTag = []byte("bamboo-fin")

func (db *DB) NewAtomicWrite(option WriteOptions) *atomicWrite {
	return &atomicWrite{
		muLock:      new(sync.RWMutex),
		options:     option,
		db:          db,
		dataToWrite: make(map[string]*content.LogStruct),
	}
}

func (aw *atomicWrite) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	aw.muLock.Lock()
	defer aw.muLock.Unlock()

	// put data to write map, pending for write
	aw.dataToWrite[string(key)] = &content.LogStruct{Key: key, Value: value}
	return nil
}

func (aw *atomicWrite) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	aw.muLock.Lock()
	defer aw.muLock.Unlock()

	logPos := aw.db.index.Get(key)
	// if log is nil, means the key is not exist in the db
	if logPos == nil {
		if aw.dataToWrite[string(key)] == nil {
			delete(aw.dataToWrite, string(key))
		}
		return nil
	}

	// stash log
	log := &content.LogStruct{Key: key, Type: content.LogDeleted}
	aw.dataToWrite[string(key)] = log
	return nil
}

// 1. put stashed log to log file
// 2. update memory index
func (aw *atomicWrite) Commit() error {
	aw.muLock.Lock()
	defer aw.muLock.Unlock()

	// has no data to write
	if len(aw.dataToWrite) == 0 {
		return nil
	}

	if uint(len(aw.dataToWrite)) > aw.options.MaxWriteCount {
		return ErrDataExceedAtomicMaxSize
	}

	// add lock to avoid other write operation
	aw.db.muLock.Lock()
	defer aw.db.muLock.Unlock()

	// get the last seqNo
	lastSeqNo := atomic.AddUint64(&aw.db.atomicSeq, 1)

	// put data to disk
	indexers := make(map[string]*content.LogStructIndex)
	for _, rec := range aw.dataToWrite {
		currentData := &content.LogStruct{
			Key:   encodeLogKeyWithSeqNo(rec.Key, lastSeqNo),
			Value: rec.Value,
			Type:  rec.Type,
		}

		logIndexer, err := aw.db.appendLog(currentData)
		if err != nil {
			return err
		}

		// in order to update memory index
		indexers[string(rec.Key)] = logIndexer
	}

	// write finishedTag to log file
	finishedRecord := &content.LogStruct{
		Key:  encodeLogKeyWithSeqNo(finishedTag, lastSeqNo),
		Type: content.LogAtomicFinish,
	}

	_, err := aw.db.appendLog(finishedRecord)
	if err != nil {
		return err
	}

	// if Sync
	if aw.options.SyncCommit && aw.db.activeBlock != nil {
		if err := aw.db.activeBlock.Sync(); err != nil {
			return err
		}
	}

	// update memory index
	for _, rec := range aw.dataToWrite {
		indexer := indexers[string(rec.Key)]
		var oldIndexer *content.LogStructIndex
		if rec.Type == content.LogDeleted {
			oldIndexer, _ = aw.db.index.Delete(rec.Key)
		} else {
			oldIndexer = aw.db.index.Put(rec.Key, indexer)
		}

		// add size to db.sizeToCollect
		if oldIndexer != nil {
			aw.db.spaceToCollect += int64(oldIndexer.DiskByteUsage)
		}
	}

	// clear data to write
	aw.dataToWrite = make(map[string]*content.LogStruct)
	return nil
}

// | seqNo |     key     |
// | n     | len(key)    |
func encodeLogKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, seqNo)

	encodedKey := make([]byte, n+len(key))
	copy(encodedKey[:n], buf[:n])
	copy(encodedKey[n:], key)

	return encodedKey
}

func parseLogKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	dataKey := key[n:]
	return dataKey, seqNo
}
