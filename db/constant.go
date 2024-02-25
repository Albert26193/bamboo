package db

import "errors"

var (
	ErrEmptyKey                = errors.New("empty key")
	ErrIndexUpdateFailed       = errors.New("failed to update index")
	ErrKeyNotFound             = errors.New("key not found")
	ErrDataFileNotFound        = errors.New("data file not found")
	ErrDataDirectory           = errors.New("data directory error")
	ErrDataExceedAtomicMaxSize = errors.New("data exceed atomic max size")
)

const initialTransactionSeq uint64 = 0
