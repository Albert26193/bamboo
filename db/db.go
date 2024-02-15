package db

import (
	"sync"
	"tiny-bitcask/content"
	"tiny-bitcask/index"
)

type DB struct {
	options       Options
	muLock        *sync.RWMutex
	activeBlock   *content.DataFile
	inactiveBlock map[uint32]*content.DataFile
	index         index.Indexer
}

func (db *DB) setActiveFile() error {
	var initialFileIndex uint32 = 0
	if db.activeBlock != nil {
		initialFileIndex = db.activeBlock.FileIndex + 1
	}

	newFile, err := content.OpenFile(db.options.DataDir, initialFileIndex)
	if err != nil {
		panic(err)
	}

	db.activeBlock = newFile
	return nil
}

func (db *DB) appendLog(log *content.LogStruct) (*content.LogStructIndex, error) {
	db.muLock.Lock()
	defer db.muLock.Unlock()

	// if empty
	if db.activeBlock == nil {
		if err := db.setActiveFile(); err != nil {
			panic(err)
		}
	}

	// write log to active block
	encodeLog, size := content.Encoder(log)

	// if reach the max size
	if db.activeBlock.WritePos+size > int64(db.options.DataSize) {
		// sync and close the active block
		if err := db.activeBlock.Sync(); err != nil {
			panic(err)
		}

		// transfer active block to inactive block
		db.inactiveBlock[db.activeBlock.FileIndex] = db.activeBlock

		// create a new active block
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}

	writePos := db.activeBlock.WritePos
	if err := db.activeBlock.Write(encodeLog); err != nil {
		return nil, err
	}
	// if sync data
	if db.options.SyncData {
		if err := db.activeBlock.Sync(); err != nil {
			return nil, err
		}
	}

	// build index
	logIndex := &content.LogStructIndex{
		FileIndex: db.activeBlock.FileIndex,
		Offset:    writePos,
	}

	return logIndex, nil
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	logStruct := &content.LogStruct{
		Key:   key,
		Value: value,
		Type:  content.LogStructCnt,
	}

	// append log to active block
	pos, err := db.appendLog(logStruct)
	if err != nil {
		return err
	}

	// update index
	if succeed := db.index.Put(key, pos); !succeed {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.muLock.RLock()
	defer db.muLock.RUnlock()

	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	// get index
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrDataFileNotFound
	}

	// find log
	var dataFile *content.DataFile
	if db.activeBlock.FileIndex == pos.FileIndex {
		dataFile = db.activeBlock
	} else {
		dataFile = db.inactiveBlock[pos.FileIndex]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// get data
	log, err := dataFile.ReadLog(pos.Offset)
	if err != nil {
		return nil, err
	}

	if log.Type == content.LogStructDeleted {
		return nil, ErrDataFileNotFound
	}

	return log.Value, nil
}
