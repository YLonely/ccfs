package cache

type Config struct {
	Directory              string `json:"directory"`
	Level1MaxLRUCacheEntry int    `json:"level1MaxLRUCacheEntry"`
	MaxLRUCacheEntry       int    `json:"maxLRUCacheEntry"`
	GCInterval             int    `json:"gcInterval"`
}
