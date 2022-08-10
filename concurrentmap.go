package main

import "sync"

// This concurrent map is similar to https://github.com/orcaman/concurrent-map
// except that the locking of map shards is performed manually instead of automatically
// this is because sometimes we need to perform I/O while the map shard is locked

type ConcurrentMap[V any] []*MapShard[V]

type MapShard[V any] struct {
	items map[string]V
	mu    *sync.Mutex
}

// Create a new map of maps
func NewConcurrentMap[V any](shards int) *ConcurrentMap[V] {
	m := make(ConcurrentMap[V], SHARDS)

	for i := 0; i < SHARDS; i++ {
		m[i] = &MapShard[V]{
			make(map[string]V),
			&sync.Mutex{},
		}
	}
	return &m
}

// AccessShard locks and returns the lock for the relevant shard
func (m ConcurrentMap[V]) AccessShard(key string) *sync.Mutex {
	shard := m.getShard(key)
	shard.mu.Lock()
	return shard.mu
}

// Set sets a value
func (m ConcurrentMap[V]) Set(key string, value V) {
	shard := m.getShard(key)
	shard.items[key] = value
}

// Mset merges multiple maps
func (m ConcurrentMap[V]) MSet(data map[string]V) {
	for key, value := range data {
		shard := m.getShard(key)
		shard.items[key] = value
	}
}

// Get gets a value
func (m ConcurrentMap[V]) Get(key string) (V, bool) {
	shard := m.getShard(key)
	val, ok := shard.items[key]
	return val, ok
}

// Delete removes a value
func (m ConcurrentMap[V]) Delete(key string) {
	shard := m.getShard(key)
	delete(shard.items, key)
}

func (m ConcurrentMap[V]) getShard(key string) *MapShard[V] {
	return m[uint(fnv32(key))%uint(SHARDS)]
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
