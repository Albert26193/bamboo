package db

import (
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
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
	fileList      []int
	atomicSeq     uint64
}

func CreateDB(options Options) (*DB, error) {
	// validate options
	if err := validateOptions(options); err != nil {
		return nil, err
	}

	// judge if the data directory exists
	if _, err := os.Stat(options.DataDir); os.IsNotExist(err) {
		if err := os.Mkdir(options.DataDir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		options:       options,
		muLock:        new(sync.RWMutex),
		inactiveBlock: make(map[uint32]*content.DataFile),
		index:         index.NewIndexer(options.IndexType),
	}

	// load data from disk
	if err := db.loadFromDisk(); err != nil {
		return nil, err
	}

	// update memory index
	if err := db.updateMemoryIndex(); err != nil {
		return nil, err
	}

	return db, nil
}

func validateOptions(options Options) error {
	if options.DataDir == "" {
		return errors.New("DataDir is empty")
	}

	if options.DataSize <= 0 {
		return errors.New("DataSize is not positive")
	}

	return nil
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

func (db *DB) lockedAppendLog(log *content.LogStruct) (*content.LogStructIndex, error) {
	db.muLock.Lock()
	defer db.muLock.Unlock()
	return db.appendLog(log)
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	logStruct := &content.LogStruct{
		Key:   encodeLogKeyWithSeqNo(key, initialTransactionSeq),
		Value: value,
		Type:  content.LogNormal,
	}

	// append log to active block
	pos, err := db.lockedAppendLog(logStruct)
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
	logStruct := db.index.Get(key)
	if logStruct == nil {
		return nil, ErrKeyNotFound
	}

	return db.GetValueFormLog(logStruct)
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// add tag to log
	log := &content.LogStruct{
		Key:  encodeLogKeyWithSeqNo(key, initialTransactionSeq),
		Type: content.LogDeleted,
	}

	// add log to active block
	_, err := db.lockedAppendLog(log)
	if err != nil {
		return err
	}

	//remove key
	if succeed := db.index.Delete(key); !succeed {
		return ErrIndexUpdateFailed
	}

	return nil
}

func (db *DB) loadFromDisk() error {
	dir, err := os.ReadDir(db.options.DataDir)
	if err != nil {
		return err
	}

	var fileList []int

	for _, dir := range dir {
		if strings.HasSuffix(dir.Name(), ".btdata") {
			fileNames := strings.Split(dir.Name(), ".")
			fileIndex, err := strconv.Atoi(fileNames[0])

			if err != nil {
				return ErrDataDirectory
			}
			fileList = append(fileList, fileIndex)
		}
	}

	// sort fileList
	sort.Ints(fileList)
	db.fileList = fileList

	// load index
	for i, fileIndex := range fileList {
		dataFile, err := content.OpenFile(db.options.DataDir, uint32(fileIndex))
		if err != nil {
			return err
		}

		if i == len(fileList)-1 {
			db.activeBlock = dataFile
		} else {
			db.inactiveBlock[uint32(fileIndex)] = dataFile
		}
	}
	return nil
}

func (db *DB) updateMemoryIndex() error {
	// empty db
	if len(db.fileList) == 0 {
		return nil
	}

	transactionMap := make(map[uint64][]*content.TransActionLog)
	var currentTransactionSeq = initialTransactionSeq

	var updateIndexFromType = func(key []byte, logType content.LogType, logPos *content.LogStructIndex) {
		succeed := false
		if logType == content.LogDeleted {
			succeed = db.index.Delete(key)
		} else {
			succeed = db.index.Put(key, logPos)
		}

		if !succeed {
			panic("update index failed")
		}
	}

	// visit each file
	for i, fileIndex := range db.fileList {
		var curIndex = uint32(fileIndex)
		var curDataFile *content.DataFile
		if curIndex == db.activeBlock.FileIndex {
			curDataFile = db.activeBlock
		} else {
			curDataFile = db.inactiveBlock[curIndex]
		}

		// read log
		offset := int64(0)
		for {
			log, size, err := curDataFile.ReadLog(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// update memory index
			logPos := &content.LogStructIndex{
				FileIndex: curIndex,
				Offset:    offset,
			}

			// get transaction seq
			dataKey, seqNo := parseLogKey(log.Key)
			// no transaction
			if seqNo == initialTransactionSeq {
				updateIndexFromType(dataKey, log.Type, logPos)
			} else {
				// if finish the transaction
				if log.Type == content.LogAtomicFinish {
					for _, transLog := range transactionMap[seqNo] {
						updateIndexFromType(transLog.Log.Key, transLog.Log.Type, transLog.Position)
					}
					delete(transactionMap, seqNo)
				} else {
					log.Key = dataKey
					transactionMap[seqNo] = append(transactionMap[seqNo], &content.TransActionLog{
						Log:      log,
						Position: logPos,
					})
				}
			}

			// update transaction seq
			if seqNo > currentTransactionSeq {
				currentTransactionSeq = seqNo
			}
			offset += size
		}

		if i == len(db.fileList)-1 {
			db.activeBlock.WritePos = offset
		}
	}

	db.atomicSeq = currentTransactionSeq
	return nil
}

func (db *DB) GetValueFormLog(logPos *content.LogStructIndex) ([]byte, error) {
	var fileToFind *content.DataFile

	if db.activeBlock.FileIndex == logPos.FileIndex {
		fileToFind = db.activeBlock
	} else {
		fileToFind = db.inactiveBlock[logPos.FileIndex]
	}

	if fileToFind == nil {
		return nil, ErrDataFileNotFound
	}

	// get data from offset
	log, _, err := fileToFind.ReadLog(logPos.Offset)
	if err != nil {
		return nil, err
	}

	if log.Type == content.LogDeleted {
		return nil, ErrKeyNotFound
	}

	return log.Value, nil
}

func (db *DB) Sync() error {
	if db.activeBlock == nil {
		return nil
	}
	db.muLock.Lock()
	defer db.muLock.Unlock()
	return db.activeBlock.Sync()
}

func (db *DB) Close() error {
	if db.activeBlock == nil {
		return nil
	}
	db.muLock.Lock()
	defer db.muLock.Unlock()

	// close active block
	if err := db.activeBlock.Close(); err != nil {
		return err
	}

	// close inactive block
	for _, file := range db.inactiveBlock {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) ListKeys() [][]byte {
	Iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())

	var index int

	Iterator.Rewind()
	for Iterator.Valid() {
		keys[index] = Iterator.Key()
		index++
		Iterator.Next()
	}

	return keys
}

func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.muLock.RLock()
	defer db.muLock.RUnlock()

	Iterator := db.index.Iterator(false)
	for Iterator.Rewind(); Iterator.Valid(); Iterator.Next() {
		value, err := db.GetValueFormLog(Iterator.Value())
		if err != nil {
			return err
		}

		key := Iterator.Key()
		if !fn(key, value) {
			break
		}
	}
	return nil
}

// destroyDB
func destroyDB(db *DB) {
	if db != nil {
		if db.activeBlock != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.options.DataDir)
		if err != nil {
			panic(err)
		}
	}
}
