package bench

import (
	"math/rand"
	"os"

	"bamboo/db"
	"bamboo/db/utils"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var dbInstance *db.DB

func init() {
	options := db.DefaultOptions
	dir, _ := os.MkdirTemp("/tmp", "bamboo-bench")
	options.DataDir = dir

	var err error
	dbInstance, err = db.CreateDB(options)
	if err != nil {
		panic(err)
	}
}

func BenchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := dbInstance.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < 100000; i++ {
		err := dbInstance.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	// fix it
	rand.NewSource(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := dbInstance.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != db.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.NewSource(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := dbInstance.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
