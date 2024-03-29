package utils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackupDir(t *testing.T) {
	baseDir := "/tmp"

	srcDir, _ := os.MkdirTemp(baseDir, "bamboo_src")
	distDir, _ := os.MkdirTemp(baseDir, "bamboo_dist")
	excludeDirs := []string{}

	err := BackupDir(srcDir, distDir, excludeDirs)
	assert.Nil(t, err)
}
