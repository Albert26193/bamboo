package db

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"tiny-bitcask/content"
	"tiny-bitcask/db/utils"
)

func (db *DB) Merge() error {
	if db.activeBlock == nil {
		return nil
	}

	db.muLock.Lock()

	// if is merging, return
	if db.inMergeProcess {
		db.muLock.Unlock()
		return ErrMergeFailed
	}

	// check if reach merge threshold
	totalDirSize, err := utils.GetDirSize(db.options.DataDir)
	if err != nil {
		db.muLock.Unlock()
		return err
	}
	curRatio := float32(db.spaceToCollect) / float32(totalDirSize)
	if curRatio < db.options.MergeThreshold {
		db.muLock.Unlock()
		return ErrMergeNotReach
	}

	// check if has enough space to merge
	availableDiskSpace, err := utils.GetAvailableDiskSpace()
	if err != nil {
		db.muLock.Unlock()
		return err
	}
	if uint64(totalDirSize-db.spaceToCollect) >= availableDiskSpace {
		db.muLock.Unlock()
		return ErrMergeSizeNotEnough
	}

	db.inMergeProcess = true
	defer func() {
		db.inMergeProcess = false
	}()

	// Sync active block
	if err := db.activeBlock.Sync(); err != nil {
		db.muLock.Unlock()
		return err
	}

	// close active block
	db.inactiveBlock[db.activeBlock.FileIndex] = db.activeBlock
	// open new active block
	if err := db.setActiveBlock(); err != nil {
		db.muLock.Unlock()
		return nil
	}

	exceptFileIndex := db.activeBlock.FileIndex

	// get all files to merge
	filesToMerge := make([]*content.BlockFile, 0)
	for _, file := range db.inactiveBlock {
		filesToMerge = append(filesToMerge, file)
	}
	db.muLock.Unlock()

	// sort and merge
	sort.Slice(filesToMerge, func(i, j int) bool {
		return filesToMerge[i].FileIndex < filesToMerge[j].FileIndex
	})

	mergePath := db.getMergePath()

	// if has merge dir, remove it
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// create merge dir
	if err := os.Mkdir(mergePath, os.ModePerm); err != nil {
		return err
	}

	// open a new bitcask engine
	mergeOptions := db.options
	mergeOptions.DataDir = mergePath
	mergeOptions.SyncData = false
	mergeEngine, err := CreateDB(mergeOptions)
	if err != nil {
		return err
	}

	// open hint file
	hintFile, err := content.GenerateNewHintBlock(mergePath)
	if err != nil {
		return err
	}

	// traverse all files to merge
	for _, file := range filesToMerge {
		offset := int64(0)
		for {
			log, size, err := file.ReadLog(int64(offset))
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// get data key
			dataKey, _ := parseLogKey(log.Key)
			logIndexer := db.index.Get(dataKey)

			// compare with memory index
			if logIndexer != nil &&
				logIndexer.FileIndex == file.FileIndex &&
				logIndexer.Offset == offset {
				// clear transaction log
				log.Key = encodeLogKeyWithSeqNo(log.Key, initialTransactionSeq)
				indexToWrite, err := mergeEngine.appendLog(log)
				if err != nil {
					return err
				}
				err = hintFile.WriteToHintBlock(dataKey, indexToWrite)
				if err != nil {
					return err
				}
			}
			offset += size
		}
	}

	// Sync
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeEngine.Sync(); err != nil {
		return err
	}

	// write merge finished tag
	mergeFinishedBlock, err := content.GenerateMergeFinishedBlock(mergePath)
	if err != nil {
		return err
	}

	mergeLog := &content.LogStruct{
		Key:   []byte(mergeFinishedTag),
		Value: []byte(strconv.Itoa(int(exceptFileIndex))),
	}

	encodedLog, _ := content.Encoder(mergeLog)
	if err := mergeFinishedBlock.Write(encodedLog); err != nil {
		return err
	}
	if err := mergeFinishedBlock.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	targetPath := path.Dir(path.Clean(db.options.DataDir))
	base := path.Base(db.options.DataDir)
	return filepath.Join(targetPath, base+mergeDirPath)
}

// getExclusiveMergeBlock
func (db *DB) getExclusiveMergeBlockId(dir string) (uint32, error) {
	mergeFinishedFile, err := content.GenerateMergeFinishedBlock(dir)
	if err != nil {
		return 0, err
	}

	rec, _, err := mergeFinishedFile.ReadLog(0)
	if err != nil {
		return 0, err
	}

	exclusiveMergeId, err := strconv.Atoi(string(rec.Value))
	if err != nil {
		return 0, err
	}
	return uint32(exclusiveMergeId), nil
}

func (db *DB) getIndexFromHint() error {
	hintName := filepath.Join(db.options.DataDir, content.HintFileTag)

	// check if hint file exists
	if _, err := os.Stat(hintName); os.IsNotExist(err) {
		return nil
	}

	// open hint file
	hintFile, err := content.GenerateNewHintBlock(db.options.DataDir)
	if err != nil {
		return err
	}

	// get indexer from hint file
	offset := int64(0)
	for {
		log, size, err := hintFile.ReadLog(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		position := content.DecodeIndex(log.Value)
		db.index.Put(log.Key, position)
		offset += size
	}

	return nil
}

func (db *DB) getMergeBlocks() error {
	mergeDir := db.getMergePath()

	// check if merge dir exists
	if _, err := os.Stat(mergeDir); os.IsNotExist(err) {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergeDir)
	}()

	childDirs, err := os.ReadDir(mergeDir)
	if err != nil {
		return err
	}

	// check if merge has finished
	var isMergeFinished = false
	var mergeFileBlocks []string
	for _, dir := range childDirs {
		if dir.Name() == content.MergeFinishedTag {
			isMergeFinished = true
		}
		if dir.Name() == FileLockName {
			continue
		}
		mergeFileBlocks = append(mergeFileBlocks, dir.Name())
	}

	// if not finished
	if !isMergeFinished {
		return nil
	}

	exclusiveBlockId, err := db.getExclusiveMergeBlockId(mergeDir)
	if err != nil {
		return err
	}

	// remove inactive files
	for fileId := uint32(0); fileId < exclusiveBlockId; fileId++ {
		fileName := content.GetBlockName(db.options.DataDir, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// move active block to merge dir
	for _, file := range mergeFileBlocks {
		srcPath := filepath.Join(mergeDir, file)
		targetPath := filepath.Join(db.options.DataDir, file)
		if err := os.Rename(srcPath, targetPath); err != nil {
			return err
		}
	}

	return nil
}
