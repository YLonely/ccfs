package cache

type Config struct {
	Directory              string `json:"directory"`
	Level1MaxLRUCacheEntry int    `json:"level1MaxLRUCacheEntry,omitempty"`
	MaxLRUCacheEntry       int    `json:"maxLRUCacheEntry,omitempty"`
	GCInterval             int    `json:"gcInterval,omitempty"`
}
