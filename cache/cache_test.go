package cache

import (
	"crypto/sha256"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
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
		wg.Add(1)
		go func(index int) {
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
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			cached := make([]byte, int64(len(items[index].value))-items[index].offset)
			n, err := bc.FetchAt(items[index].key, items[index].offset, cached)
			if err != nil {
				t.Errorf("failed to fetch data with key %s at %d with length %d, error %s", items[index].key, items[index].offset, len(cached), err.Error())
				return
			}
			if n != len(cached) {
				t.Errorf("length of read data mismatch, get %d, expect %d", n, len(cached))
				return
			}
			if string(items[index].value[items[index].offset:]) != string(cached) {
				t.Errorf("cached data error, get %s, expect %s", cached, items[index].value[items[index].offset:])
				return
			}
		}(i)
	}
	wg.Wait()
}

func TestWeightedCache(t *testing.T) {
	if err := os.MkdirAll(testDirectory, 0644); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDirectory)
	wc, err := NewWeightedBlobCache(testDirectory, WeightedBlobCacheConfig{
		Level1MaxLRUCacheEntry: level1MaxLRUCacheEntry,
		MaxLRUCacheEntry:       maxLRUCacheEntry,
		GCInterval:             gcInterval,
	})
	if err != nil {
		t.Fatal(err)
	}
	maxItemsGroup := 3
	maxItems := 8
	itemsGroups := make([][]item, maxItemsGroup)
	for i := range itemsGroups {
		itemsGroups[i] = make([]item, 0, maxItems)
		for j := 0; j < maxItems; j++ {
			key := fmt.Sprintf("key-%d-%d", i, j)
			sha := sha256.Sum256([]byte(key))
			value := []byte(fmt.Sprintf("%s-%x", key, sha))
			itemsGroups[i] = append(itemsGroups[i], item{
				key:   key,
				value: value,
			})
		}
	}
	maxBlobCaches := maxItemsGroup
	cacheIDs := make([]string, maxBlobCaches)
	for i := range cacheIDs {
		cacheIDs[i] = fmt.Sprintf("cache%d", i)
	}
	bcs := make([]BlobCache, 0, maxBlobCaches)
	var weight float32
	for _, id := range cacheIDs {
		if err := wc.AddCache(id, weight); err != nil {
			t.Fatal(err)
		}
		bc, err := wc.GetCache(id)
		if err != nil {
			t.Fatal(err)
		}
		bcs = append(bcs, bc)
		weight++
	}
	buildCache(t, bcs, itemsGroups)
	checkEntries(t, wc, cacheIDs, bcs, maxBlobCaches, maxItems)
	// adjust weight
	newIDs := []string{}
	newbcs := []BlobCache{}
	w := 0
	t.Log("adjust weight")
	for i := len(cacheIDs) - 1; i >= 0; i-- {
		if err := wc.Adjust(cacheIDs[i], float32(w)); err != nil {
			t.Fatal(err)
		}
		newIDs = append(newIDs, cacheIDs[i])
		newbcs = append(newbcs, bcs[i])
		w++
	}

	// rebuild cache
	buildCache(t, bcs, itemsGroups)
	checkEntries(t, wc, newIDs, newbcs, maxBlobCaches, maxItems)
}

func checkEntries(t *testing.T, wc *WeightedBlobCache, cacheIDs []string, bcs []BlobCache, maxBlobCaches, maxItems int) {
	entriesNoGC := min(maxItems, level1MaxLRUCacheEntry)
	expectedEntries := maxBlobCaches * entriesNoGC
	currentEntries := wc.entrySize()
	diff := expectedEntries - maxLRUCacheEntry
	if currentEntries != expectedEntries {
		t.Errorf("wrong entry size after insertion, get %d, expect %d", currentEntries, expectedEntries)
	}
	time.Sleep(time.Second * time.Duration(gcInterval+1))
	currentEntries = wc.entrySize()
	expectedEntries = maxLRUCacheEntry
	if currentEntries != expectedEntries {
		t.Errorf("wrong entry size after gc, get %d, expect %d", currentEntries, expectedEntries)
	}
	for i := range bcs {
		cacheEntries := bcs[i].(*blobCache).len()
		diff, expectedEntries = calcExpect(diff, entriesNoGC)
		if expectedEntries != cacheEntries {
			t.Errorf("%s has wrong size of entries, get %d, expect %d", cacheIDs[i], cacheEntries, expectedEntries)
		}
	}
}

func buildCache(t *testing.T, bcs []BlobCache, itemsGroups [][]item) {
	wg := sync.WaitGroup{}
	for i := range bcs {
		for _, it := range itemsGroups[i] {
			wg.Add(1)
			go func(index int, itt item) {
				defer wg.Done()
				if err := bcs[index].Add(itt.key, itt.value); err != nil {
					t.Errorf("failed to add item with key %s value %s", itt.key, itt.value)
				}
			}(i, it)
		}
	}
	wg.Wait()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func calcExpect(diff, entries int) (newDiff, exp int) {
	if diff <= 0 {
		return 0, entries
	}
	if diff >= entries {
		return diff - entries, 0
	}
	return 0, entries - diff
}
