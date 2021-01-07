package cache

import (
	"bytes"
	"io"
	"os"
	"path"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"
)

const (
	defaultLevel1MaxLRUCacheEntry = 25
	defaultMaxLRUCacheEntry       = 1 << 10
)

// BlobCache caches data
type BlobCache interface {
	Add(key string, p []byte) error
	FetchAt(key string, offset int64, p []byte) (n int, err error)
}

func NewBlobCache(directory string, maxEntries int) (BlobCache, error) {
	if maxEntries <= 0 {
		maxEntries = defaultLevel1MaxLRUCacheEntry
	}
	if err := os.MkdirAll(directory, os.ModePerm); err != nil {
		return nil, err
	}
	bc := &blobCache{
		cache:     newObjectCache(maxEntries),
		directory: directory,
		tmpLock:   &namedLock{},
		bufPool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
	return bc, nil
}

// blobCache is a cache whose backend are files, but it also use memory
// to cache data for fast fetching
type blobCache struct {
	cache     *objectCache
	directory string
	tmpLock   *namedLock
	bufPool   sync.Pool
}

func (bc *blobCache) FetchAt(key string, offset int64, p []byte) (n int, err error) {
	// get cached data from memory
	if b, done, ok := bc.cache.get(key); ok {
		defer done()
		data := b.(*bytes.Buffer).Bytes()
		if int64(len(data)) < offset {
			return 0, errors.Errorf("invalid offset %d exceeds chunk size %d", offset, len(data))
		}
		return copy(p, data[offset:]), nil
	}
	// get cached data from file
	file, err := os.Open(bc.cachePath(key))
	if err != nil {
		return 0, errors.Wrapf(err, "failed to open cached file for %q", key)
	}
	defer file.Close()
	if n, err = file.ReadAt(p, offset); err == io.EOF {
		err = nil
	}
	return n, err
}

func (bc *blobCache) Add(key string, p []byte) error {
	// cache the data to memory
	release := false
	b := bc.bufPool.Get().(*bytes.Buffer)
	defer func() {
		if release {
			bc.bufPool.Put(b)
		}
	}()
	b.Reset()
	b.Grow(len(p))
	b.Write(p)
	if !bc.cache.add(key, newObject(b, func(v interface{}) { bc.bufPool.Put(b) })) {
		release = true
	}
	// cache the data to disk
	cp, tp := bc.cachePath(key), bc.tmpPath(key)
	bc.tmpLock.lock(key)
	if _, err := os.Stat(tp); err == nil {
		bc.tmpLock.unlock(key)
		// write in progress
		return nil
	}
	if _, err := os.Stat(cp); err == nil {
		bc.tmpLock.unlock(key)
		// already exists
		return nil
	}
	if err := os.MkdirAll(path.Dir(tp), os.ModePerm); err != nil {
		bc.tmpLock.unlock(key)
		return errors.Wrapf(err, "failed to create tmp cache directory %s", path.Dir(tp))
	}
	tmpFile, err := os.Create(tp)
	if err != nil {
		bc.tmpLock.unlock(key)
		return errors.Wrapf(err, "failed to create tmp cache file %s", tp)
	}
	bc.tmpLock.unlock(key)
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()
	expected := len(p)
	reader := bytes.NewReader(p)
	if _, err := io.CopyN(tmpFile, reader, int64(expected)); err != nil {
		return errors.Wrap(err, "failed to write data to tmp file")
	}
	// commit
	if err := os.MkdirAll(path.Dir(cp), os.ModePerm); err != nil {
		return errors.Wrapf(err, "failed to create cache directory %s", path.Dir(cp))
	}
	if err := os.Rename(tmpFile.Name(), cp); err != nil {
		return errors.Wrapf(err, "failed to commit the cache file %s", cp)
	}
	return nil
}

func (bc *blobCache) len() int {
	return bc.cache.cache.Len()
}

func (bc *blobCache) reduce(n int) int {
	return bc.cache.reduce(n)
}

func (bc *blobCache) cachePath(key string) string {
	return path.Join(bc.directory, key[:2], key)
}

func (bc *blobCache) tmpPath(key string) string {
	return path.Join(bc.directory, key[:2], "tmp", key)
}

func newObjectCache(maxEntries int) *objectCache {
	oc := &objectCache{
		cache: lru.New(maxEntries),
	}
	oc.cache.OnEvicted = func(key lru.Key, value interface{}) {
		value.(*object).release()
	}
	return oc
}

type objectCache struct {
	cache *lru.Cache
	mu    sync.Mutex
}

func (oc *objectCache) get(key string) (value interface{}, done func(), ok bool) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	o, ok := oc.cache.Get(key)
	if !ok {
		return nil, nil, false
	}
	o.(*object).use()
	return o.(*object).v, func() { o.(*object).release() }, true
}

func (oc *objectCache) add(key string, obj *object) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if _, ok := oc.cache.Get(key); ok {
		return false
	}
	obj.use()
	oc.cache.Add(key, obj)
	return true
}

func (oc *objectCache) reduce(n int) int {
	if n <= 0 {
		return 0
	}
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if n > oc.cache.Len() {
		n = oc.cache.Len()
	}
	for i := 0; i < n; i++ {
		oc.cache.RemoveOldest()
	}
	return n
}

func newObject(v interface{}, destructor func(interface{})) *object {
	return &object{
		v:          v,
		destructor: destructor,
	}
}

type object struct {
	v          interface{}
	refCounts  int64
	destructor func(interface{})
	sync.Mutex
}

func (o *object) use() {
	o.Lock()
	defer o.Unlock()
	o.refCounts++
}

func (o *object) release() {
	o.Lock()
	defer o.Unlock()
	o.refCounts--
	if o.refCounts <= 0 && o.destructor != nil {
		o.destructor(o.v)
	}
}

type namedLock struct {
	muMap  map[string]*sync.Mutex
	refMap map[string]int

	mu sync.Mutex
}

func (nl *namedLock) lock(name string) {
	nl.mu.Lock()
	if nl.muMap == nil {
		nl.muMap = make(map[string]*sync.Mutex)
	}
	if nl.refMap == nil {
		nl.refMap = make(map[string]int)
	}
	if _, ok := nl.muMap[name]; !ok {
		nl.muMap[name] = &sync.Mutex{}
	}
	mu := nl.muMap[name]
	nl.refMap[name]++
	nl.mu.Unlock()
	mu.Lock()
}

func (nl *namedLock) unlock(name string) {
	nl.mu.Lock()
	mu := nl.muMap[name]
	nl.refMap[name]--
	if nl.refMap[name] <= 0 {
		delete(nl.muMap, name)
		delete(nl.refMap, name)
	}
	nl.mu.Unlock()
	mu.Unlock()
}
