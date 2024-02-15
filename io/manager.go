package io

type IOManager interface {
	Read([]byte, int64) (int, error)

	Write([]byte) (int, error)

	Sync() error

	Close() error
}

const DataFileMode = 0644
