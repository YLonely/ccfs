package cache

import (
	"container/heap"
	"os"
	"path"
	"sync"
	"time"
)

const (
	DefaultGCInterval = 5
)

// NewWeightedBlobCache returns a new WeightedBlobCache
func NewWeightedBlobCache(directory string, config WeightedBlobCacheConfig) (*WeightedBlobCache, error) {
	if config.Level1MaxLRUCacheEntry <= 0 {
		config.Level1MaxLRUCacheEntry = DefaultLevel1MaxLRUCacheEntry
	}
	if config.MaxLRUCacheEntry <= 0 {
		config.MaxLRUCacheEntry = DefaultMaxLRUCacheEntry
	}
	if config.GCInterval <= 0 {
		config.GCInterval = DefaultGCInterval
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
	mu        sync.Mutex
}

func (c *WeightedBlobCache) AddCache(id string, weight float32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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

func (c *WeightedBlobCache) Add(id, key string, data []byte) error {
	c.mu.Lock()
	cw, exists := c.caches[id]
	if !exists {
		c.mu.Unlock()
		return os.ErrNotExist
	}
	c.mu.Unlock()
	return cw.cache.Add(key, data)
}

func (c *WeightedBlobCache) FetchAt(id, key string, offset int64, data []byte) (int, error) {
	c.mu.Lock()
	cw, exists := c.caches[id]
	if !exists {
		c.mu.Unlock()
		return 0, os.ErrNotExist
	}
	c.mu.Unlock()
	opts := []Option{}
	if cw.cacheToMemory {
		opts = append(opts, WithCacheToMemory)
	}
	return cw.cache.FetchAt(key, offset, data, opts...)
}

func (c *WeightedBlobCache) Adjust(id string, weight float32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cw, exists := c.caches[id]
	if !exists {
		return os.ErrNotExist
	}
	cw.weight = weight
	cw.cacheToMemory = true
	heap.Fix(&c.hp, cw.index)
	return nil
}

func (c *WeightedBlobCache) entrySize() int {
	current := 0
	for _, cw := range c.caches {
		current += cw.cache.len()
	}
	return current
}

func (c *WeightedBlobCache) cacheGC() {
	if c.config.GCInterval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Second * time.Duration(c.config.GCInterval))
	for {
		currentEntries := c.entrySize()
		if currentEntries > c.config.MaxLRUCacheEntry {
			c.mu.Lock()
			diff := currentEntries - c.config.MaxLRUCacheEntry
			cws := make([]*cacheWrapper, 0, diff/c.config.Level1MaxLRUCacheEntry+1)
			for diff > 0 {
				lowestWeightItem := heap.Pop(&c.hp).(*cacheWrapper)
				// do not re-cache to memory to prevent the fluctuation of cache entries in mem
				lowestWeightItem.cacheToMemory = false
				diff -= lowestWeightItem.cache.reduce(diff)
				cws = append(cws, lowestWeightItem)
			}
			for _, cw := range cws {
				heap.Push(&c.hp, cw)
			}
			c.mu.Unlock()
		}
		<-ticker.C
	}
}

type WeightedBlobCacheConfig struct {
	Level1MaxLRUCacheEntry int
	MaxLRUCacheEntry       int
	GCInterval             int
}

type cacheWrapper struct {
	cache         *blobCache
	index         int
	weight        float32
	cacheToMemory bool
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
