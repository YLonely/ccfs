package cache

import (
	"container/heap"
	"os"
	"path"
	"sync"
	"time"
)

const (
	defaultGCInterval = 5
)

// NewWeightedBlobCache returns a new WeightedBlobCache
func NewWeightedBlobCache(directory string, config WeightedBlobCacheConfig) (*WeightedBlobCache, error) {
	if config.Level1MaxLRUCacheEntry == 0 {
		config.Level1MaxLRUCacheEntry = defaultLevel1MaxLRUCacheEntry
	}
	if config.MaxLRUCacheEntry == 0 {
		config.MaxLRUCacheEntry = defaultMaxLRUCacheEntry
	}
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}
	ret := &WeightedBlobCache{
		caches:    map[string]*cacheWrapper{},
		directory: directory,
		config:    config,
	}
	heap.Init(&ret.hp)
	go ret.cacheGC()
	return ret, nil
}

// WeightedBlobCache is a cache implementation which takes the weight of different cache
// groups into account during cache managing
type WeightedBlobCache struct {
	hp        cacheHeap
	caches    map[string]*cacheWrapper
	directory string
	config    WeightedBlobCacheConfig
	sync.Mutex
}

func (c *WeightedBlobCache) AddCache(id string, weight float32) error {
	c.Lock()
	defer c.Unlock()
	if _, exists := c.caches[id]; exists {
		return nil
	}
	cw := &cacheWrapper{
		weight: weight,
	}
	bc, err := NewBlobCache(path.Join(c.directory, id), c.config.Level1MaxLRUCacheEntry)
	if err != nil {
		return err
	}
	cw.cache = bc.(*blobCache)
	heap.Push(&c.hp, cw)
	c.caches[id] = cw
	return nil
}

func (c *WeightedBlobCache) GetCache(id string) (BlobCache, error) {
	c.Lock()
	defer c.Unlock()
	cw, exists := c.caches[id]
	if !exists {
		return nil, os.ErrNotExist
	}
	return cw.cache, nil
}

func (c *WeightedBlobCache) Adjust(id string, weight float32) error {
	c.Lock()
	defer c.Unlock()
	cw, exists := c.caches[id]
	if !exists {
		return os.ErrNotExist
	}
	cw.weight = weight
	heap.Fix(&c.hp, cw.index)
	return nil
}

func (c *WeightedBlobCache) cacheGC() {
	if c.config.gcInterval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Second * time.Duration(c.config.gcInterval))
	for {
		currentEntries := 0
		for _, cw := range c.caches {
			currentEntries += cw.cache.len()
		}
		if currentEntries > c.config.MaxLRUCacheEntry {
			c.Lock()
			diff := currentEntries - c.config.MaxLRUCacheEntry
			cws := make([]*cacheWrapper, 0, diff/c.config.Level1MaxLRUCacheEntry+1)
			for diff > 0 {
				lowestWeightItem := heap.Pop(&c.hp).(*cacheWrapper)
				diff -= lowestWeightItem.cache.reduce(diff)
				cws = append(cws, lowestWeightItem)
			}
			for _, cw := range cws {
				heap.Push(&c.hp, cw)
			}
			c.Unlock()
		}
		<-ticker.C
	}
}

type WeightedBlobCacheConfig struct {
	Level1MaxLRUCacheEntry int
	MaxLRUCacheEntry       int
	gcInterval             int
}

type cacheWrapper struct {
	cache  *blobCache
	index  int
	weight float32
}

type cacheHeap []*cacheWrapper

func (h cacheHeap) Len() int           { return len(h) }
func (h cacheHeap) Less(i, j int) bool { return h[i].weight < h[j].weight }
func (h cacheHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *cacheHeap) Push(x interface{}) {
	n := len(*h)
	obj := x.(*cacheWrapper)
	obj.index = n
	*h = append(*h, obj)
}
func (h *cacheHeap) Pop() interface{} {
	n := len(*h)
	obj := (*h)[n-1]
	obj.index = -1
	(*h)[n-1] = nil
	*h = (*h)[:n-1]
	return obj
}
