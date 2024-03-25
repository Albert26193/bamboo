package bench

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"bamboo/db"
	"bamboo/db/utils"

	"github.com/stretchr/testify/assert"
)

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

func BenchmarkGoroutinePut(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := dbInstance.Put(utils.GetTestKey(i), utils.ConcurrencyRandomValue(1024))
			assert.Nil(b, err)
		}(i)
	}
	wg.Wait()
}

func BenchmarkGoroutineGet(b *testing.B) {
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := dbInstance.Put(utils.GetTestKey(i), utils.ConcurrencyRandomValue(1024))
			assert.Nil(b, err)
		}(i)
	}
	wg.Wait()

	b.ResetTimer()
	b.ReportAllocs()

	rand.NewSource(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := dbInstance.Get(utils.GetTestKey(rand.Int()))
			if err != nil && err != db.ErrKeyNotFound {
				assert.Nil(b, err)
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkGoroutineDelete(b *testing.B) {
	var wg sync.WaitGroup
	b.ResetTimer()
	b.ReportAllocs()

	rand.NewSource(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			err := dbInstance.Delete(utils.GetTestKey(rand.Int()))
			assert.Nil(b, err)
		}(i)
	}
	wg.Wait()
}
