package utils

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

func BackupDir(srcDir string, distDir string, excludeDirs []string) error {
	// check if dist dir exists
	if _, err := os.Stat(distDir); os.IsNotExist(err) {
		if err := os.MkdirAll(distDir, os.ModePerm); err != nil {
			return err
		}
	}

	// check if srcDir exists
	if _, err := os.Stat(srcDir); os.IsNotExist(err) {
		return errors.New("source dir not exists")
	}

	return filepath.Walk(srcDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		name := strings.Replace(path, srcDir, "", 1)
		if name == "" {
			return nil
		}

		for _, excludeDir := range excludeDirs {
			isMatched, err := filepath.Match(excludeDir, info.Name())
			if err != nil {
				return err
			}

			if isMatched {
				return nil
			}
		}

		if info.IsDir() {
			// create dir
			return os.MkdirAll(filepath.Join(distDir, name), os.ModePerm)
		}

		store, err := os.ReadFile(filepath.Join(srcDir, name))
		if err != nil {
			return err
		}

		return os.WriteFile(filepath.Join(distDir, name), store, info.Mode())
	})
}
