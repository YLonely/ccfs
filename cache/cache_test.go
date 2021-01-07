package cache

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"
)

const (
	testDirectory          = "./cache-test"
	level1MaxLRUCacheEntry = 10
	maxLRUCacheEntry       = 5
	gcInterval             = 1
)

type item struct {
	key    string
	value  []byte
	offset int64
}

func TestBlobCache(t *testing.T) {
	if err := os.MkdirAll(testDirectory, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDirectory)
	wg := sync.WaitGroup{}
	bc, err := NewBlobCache(testDirectory, level1MaxLRUCacheEntry)
	if err != nil {
		t.Fatal(err)
	}
	items := []item{}
	cnt := 20
	for i := 0; i < cnt; i++ {
		k := fmt.Sprintf("key%d", i)
		sha := sha256.Sum256([]byte(k))
		v := []byte(fmt.Sprintf("key%d-%x", i, sha))
		items = append(items,
			item{
				key:    k,
				value:  v,
				offset: int64(i),
			},
		)
	}
	for i := range items {
		go func(index int) {
			wg.Add(1)
			defer wg.Done()
			if err := bc.Add(items[index].key, items[index].value); err != nil {
				t.Errorf("failed to add key %s with value %s, error %s", items[index].key, items[index].value, err.Error())
			} else {
				t.Logf("cached data with key %s value %s", items[index].key, items[index].value)
			}
		}(i)
	}
	wg.Wait()
	for i := range items {
		go func(index int) {
			wg.Add(1)
			defer wg.Done()
			cached := make([]byte, int64(len(items[index].value))-items[index].offset)
			n, err := bc.FetchAt(items[index].key, items[index].offset, cached)
			if err != nil {
				t.Errorf("failed to fetch data with key %s at %d with length %d, error %s", items[index].key, items[index].offset, len(cached), err.Error())
			}
			if n != len(cached) {
				t.Errorf("length of read data mismatch, get %d, expect %d", n, len(cached))
			}
			if string(items[index].value[items[index].offset:]) != string(cached) {
				t.Errorf("cached data error, get %s, expect %s", cached, items[index].value[items[index].offset:])
			}
		}(i)
	}
	wg.Wait()
}
