package utils

import (
	"io/fs"
	"path/filepath"
	"syscall"
)

// get the size of the directory
func GetDirSize(dir string) (int64, error) {
	var size int64
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// get the available disk size
func GetAvailableDiskSpace() (uint64, error) {
	cur, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t
	if err = syscall.Statfs(cur, &stat); err != nil {
		return 0, err
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}
