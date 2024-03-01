package db

import (
	"bamboo/content"
	"bamboo/db/utils"
	"bamboo/diskIO"
	"bamboo/index"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gofrs/flock"
)

type DB struct {
	options        Options
	muLock         *sync.RWMutex
	activeBlock    *content.BlockFile
	inactiveBlock  map[uint32]*content.BlockFile
	index          index.Indexer
	fileList       []int
	atomicSeq      uint64
	inMergeProcess bool
	fLock          *flock.Flock
	bytesCount     uint
	spaceToCollect int64
}

// get the status of the db
// how many files, how many keys, how many bytes
type DBStatus struct {
	BlockCount     uint
	KeyCount       uint
	BytesToCollect int64
	DiskUsage      int64
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

	// lock to dir: only a process can use the db
	fLock := flock.New(filepath.Join(options.DataDir, FileLockName))
	isLocked, err := fLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !isLocked {
		return nil, ErrDBIsUsing
	}

	db := &DB{
		options:       options,
		muLock:        new(sync.RWMutex),
		inactiveBlock: make(map[uint32]*content.BlockFile),
		index:         index.NewIndexer(options.IndexType),
		fLock:         fLock,
	}

	// first, check if has merge dir
	if err := db.getMergeBlocks(); err != nil {
		return nil, err
	}

	// load data from disk
	if err := db.loadFromDisk(); err != nil {
		return nil, err
	}

	// load from hint
	if err := db.getIndexFromHint(); err != nil {
		return nil, err
	}

	// update memory index
	if err := db.updateMemoryIndex(); err != nil {
		return nil, err
	}

	// set io to system io, because need to write or sync data
	if db.options.QuickStart {
		if err := db.restoreFileSystemIO(); err != nil {
			return nil, err
		}
	}
	return db, nil
}

// why need to restore file system io?
// when the db is created, the io may be set to mmap, but when the db is closed, the io is set to system io
func (db *DB) restoreFileSystemIO() error {
	if db.activeBlock == nil {
		return nil
	}

	if err := db.activeBlock.SetIOManager(db.options.DataDir, diskIO.FileSystemIO); err != nil {
		return err
	}

	for _, dataFile := range db.inactiveBlock {
		if err := dataFile.SetIOManager(db.options.DataDir, diskIO.FileSystemIO); err != nil {
			return err
		}
	}
	return nil
}

// get the status of the db
func (db *DB) GetDBStatus() *DBStatus {
	db.muLock.RLock()
	defer db.muLock.RUnlock()

	var blocksCnt = uint(len(db.inactiveBlock))
	if db.activeBlock != nil {
		blocksCnt++
	}

	DiskUsage, err := utils.GetDirSize(db.options.DataDir)
	if err != nil {
		panic(fmt.Sprintf("GetDirSize failed, error: %v", err))
	}

	return &DBStatus{
		BlockCount:     blocksCnt,
		KeyCount:       uint(db.index.Size()),
		BytesToCollect: db.spaceToCollect,
		DiskUsage:      DiskUsage,
	}
}

func validateOptions(options Options) error {
	if options.DataDir == "" {
		return errors.New("DataDir is empty")
	}

	if options.DataSize <= 0 {
		return errors.New("DataSize is not positive")
	}

	if options.SyncThreshold <= 0 {
		return errors.New("SyncThreshold is not positive")
	}

	if options.MergeThreshold < 0 || options.MergeThreshold > 1 {
		return errors.New("MergeThreshold is not in range [0, 1]")
	}

	return nil
}

func (db *DB) setActiveBlock() error {
	var initialFileIndex uint32 = 0
	if db.activeBlock != nil {
		initialFileIndex = db.activeBlock.FileIndex + 1
	}

	newFile, err := content.OpenBlock(db.options.DataDir, initialFileIndex, diskIO.FileSystemIO)

	if err != nil {
		panic(err)
	}

	db.activeBlock = newFile
	return nil
}

// why return indexer?
// 1. append log to current active block
// 2. update index, and return new indexer
func (db *DB) appendLog(log *content.LogStruct) (*content.LogStructIndex, error) {
	// if empty
	if db.activeBlock == nil {
		if err := db.setActiveBlock(); err != nil {
			panic(err)
		}
	}

	// write log to active block
	encodeLog, size := content.Encoder(log)
	// update bytes count
	db.bytesCount += uint(size)

	// if reach the max size
	if db.activeBlock.WritePos+size > int64(db.options.DataSize) {
		// sync and close the active block
		if err := db.activeBlock.Sync(); err != nil {
			panic(err)
		}

		// transfer active block to inactive block
		db.inactiveBlock[db.activeBlock.FileIndex] = db.activeBlock

		// create a new active block
		if err := db.setActiveBlock(); err != nil {
			return nil, err
		}
	}

	// check options: weather to sync data
	if db.options.SyncData && db.bytesCount >= db.options.SyncThreshold {
		if err := db.activeBlock.Sync(); err != nil {
			return nil, err
		}
		db.bytesCount = 0
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
		FileIndex:     db.activeBlock.FileIndex,
		Offset:        writePos,
		DiskByteUsage: uint32(size),
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
	if oldIndexer := db.index.Put(key, pos); oldIndexer != nil {
		db.spaceToCollect += int64(oldIndexer.DiskByteUsage)
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
	curIndexer, err := db.lockedAppendLog(log)
	if err != nil {
		return err
	}
	// the entry is deleted, so the space need to collect
	db.spaceToCollect += int64(curIndexer.DiskByteUsage)

	//remove key
	oldIndexer, succeed := db.index.Delete(key)
	if !succeed {
		return ErrIndexUpdateFailed
	}
	if oldIndexer != nil {
		db.spaceToCollect += int64(oldIndexer.DiskByteUsage)
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
		if strings.HasSuffix(dir.Name(), content.Suffix) {
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
		// quick start: mmap
		ioType := diskIO.FileSystemIO
		if db.options.QuickStart {
			ioType = diskIO.MMapIO
		}

		dataBlock, err := content.OpenBlock(db.options.DataDir, uint32(fileIndex), ioType)

		if err != nil {
			return errors.New("error here")
		}

		if i == len(fileList)-1 {
			db.activeBlock = dataBlock
		} else {
			db.inactiveBlock[uint32(fileIndex)] = dataBlock
		}
	}
	return nil
}

func (db *DB) updateMemoryIndex() error {
	// empty db
	if len(db.fileList) == 0 {
		return nil
	}

	hasMerged, exclusiveMergeId := false, uint32(0)
	mergeFinishedName := filepath.Join(db.options.DataDir, content.MergeFinishedTag)

	if _, err := os.Stat(mergeFinishedName); err == nil {
		finId, err := db.getExclusiveMergeBlockId(db.options.DataDir)
		if err != nil {
			return err
		}
		hasMerged = true
		exclusiveMergeId = finId
	}

	var updateIndexFromType = func(key []byte, logType content.LogType, logPos *content.LogStructIndex) {
		var oldIndexer *content.LogStructIndex
		if logType == content.LogDeleted {
			oldIndexer, _ = db.index.Delete(key)
			db.spaceToCollect += int64(logPos.DiskByteUsage)
		} else {
			oldIndexer = db.index.Put(key, logPos)
		}

		if oldIndexer != nil {
			db.spaceToCollect += int64(oldIndexer.DiskByteUsage)
		}
	}

	transactionMap := make(map[uint64][]*content.TransActionLog)
	var currentTransactionSeq = initialTransactionSeq

	// visit each file
	for i, fileIndex := range db.fileList {
		var curIndex = uint32(fileIndex)

		// if has merged and not reach the exclusive merge id
		if hasMerged && curIndex < exclusiveMergeId {
			continue
		}

		var curBlockFile *content.BlockFile
		if curIndex == db.activeBlock.FileIndex {
			curBlockFile = db.activeBlock
		} else {
			curBlockFile = db.inactiveBlock[curIndex]
		}

		// read log
		offset := int64(0)
		for {
			log, size, err := curBlockFile.ReadLog(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// update memory index
			logPos := &content.LogStructIndex{
				FileIndex:     curIndex,
				Offset:        offset,
				DiskByteUsage: uint32(size),
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
	var fileToFind *content.BlockFile

	if db.activeBlock.FileIndex == logPos.FileIndex {
		fileToFind = db.activeBlock
	} else {
		fileToFind = db.inactiveBlock[logPos.FileIndex]
	}

	if fileToFind == nil {
		return nil, ErrBlockFileNotFound
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
	// unlock
	defer func() {
		if err := db.fLock.Unlock(); err != nil {
			panic(fmt.Sprintf("Lock release failed, error: %v", err))
		}
	}()

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

// backup db
func (db *DB) Backup(dir string) error {
	db.muLock.RLock()
	defer db.muLock.RUnlock()
	return utils.BackupDir(db.options.DataDir, dir, []string{FileLockName})
}
