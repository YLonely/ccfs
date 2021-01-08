package cache

type Config struct {
	Directory              string `json:"directory"`
	Level1MaxLRUCacheEntry int    `json:"cache_entry_per_checkpoint,omitempty"`
	MaxLRUCacheEntry       int    `json:"max_cache_entry,omitempty"`
	GCInterval             int    `json:"gc_interval,omitempty"`
}
