package db

import "errors"

var (
	ErrEmptyKey                = errors.New("empty key")
	ErrIndexUpdateFailed       = errors.New("failed to update index")
	ErrKeyNotFound             = errors.New("key not found")
	ErrBlockFileNotFound       = errors.New("data file not found")
	ErrDataDirectory           = errors.New("data directory error")
	ErrDataExceedAtomicMaxSize = errors.New("data exceed atomic max size")
	ErrMergeFailed             = errors.New("merge failed")
	ErrDBIsUsing               = errors.New("db is using")
)

const (
	initialTransactionSeq uint64 = 0
	mergeDirPath                 = "-BT-MERGE"
	mergeFinishedTag             = "MERGE.FINISHED"
	FileLockName                 = "IOLOCK"
)
