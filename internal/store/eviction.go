package store

import (
	"fmt"
	"math/rand"
	"time"
)

type EvictionPolicy string

const (
	EvictionNoEviction     EvictionPolicy = "noeviction"
	EvictionAllKeysLRU     EvictionPolicy = "allkeys-lru"
	EvictionVolatileLRU    EvictionPolicy = "volatile-lru"
	EvictionAllKeysLFU     EvictionPolicy = "allkeys-lfu"
	EvictionVolatileLFU    EvictionPolicy = "volatile-lfu"
	EvictionAllKeysRandom  EvictionPolicy = "allkeys-random"
	EvictionVolatileRandom EvictionPolicy = "volatile-random"
	EvictionVolatileTTL    EvictionPolicy = "volatile-ttl"
)

type EvictionConfig struct {
	MaxMemory      int64
	Policy         EvictionPolicy
	Samples        int
	currentMemory  int64
	memoryTracking bool
}

func NewEvictionConfig(maxMemory int64, policy EvictionPolicy, samples int) *EvictionConfig {
	if samples <= 0 {
		samples = 5
	}
	return &EvictionConfig{
		MaxMemory:      maxMemory,
		Policy:         policy,
		Samples:        samples,
		memoryTracking: maxMemory > 0,
	}
}

func EstimateObjectSize(obj *RedisObject) int64 {
	if obj == nil {
		return 0
	}

	size := int64(32)

	switch obj.Type {
	case ObjString:
		switch obj.Encoding {
		case EncodingInt:
			size += 8
			if str, ok := obj.Ptr.(string); ok {
				size += int64(len(str))
			}
		}

	case ObjList:
		if ql, ok := obj.Ptr.(*Quicklist); ok {
			size += int64(ql.Len() * 50)
		}

	case ObjHash:
		if ht, ok := obj.Ptr.(*HashTable); ok {
			size += int64(ht.Len() * 100)
		}

	case ObjSet:
		if s, ok := obj.Ptr.(*Set); ok {
			size += int64(s.Card() * 50)
		}

	case ObjZSet:
		if zs, ok := obj.Ptr.(*ZSet); ok {
			size += int64(zs.Card() * 100)
		}
	}

	return size
}

func EstimateKeySize(key string) int64 {
	return int64(len(key) + 32)
}

func (s *Store) PerformEviction() (string, bool) {
	if s.evictionConfig == nil || s.evictionConfig.MaxMemory == 0 {
		return "", false
	}

	switch s.evictionConfig.Policy {
	case EvictionNoEviction:
		return "", false
	case EvictionAllKeysLRU:
		return s.evictApproximateLRU(false), true
	case EvictionVolatileLRU:
		return s.evictApproximateLRU(true), true
	case EvictionAllKeysRandom:
		return s.evictRandom(false), true
	case EvictionVolatileRandom:
		return s.evictRandom(true), true
	case EvictionVolatileTTL:
		return s.evictShortestTTL(), true
	case EvictionAllKeysLFU:
		return s.evictApproximateLRU(false), true
	case EvictionVolatileLFU:
		return s.evictApproximateLRU(true), true
	default:
		return "", false
	}
}

func (s *Store) evictApproximateLRU(volatileOnly bool) string {
	samples := s.evictionConfig.Samples
	if samples <= 0 {
		samples = 5
	}

	var candidates []string
	if volatileOnly {
		for key := range s.expires {
			if _, exists := s.data[key]; exists {
				candidates = append(candidates, key)
			}
		}
	} else {
		for key := range s.data {
			candidates = append(candidates, key)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	var oldestKey string
	var oldestLRU uint32 = ^uint32(0)

	sampleSize := samples
	if len(candidates) < sampleSize {
		sampleSize = len(candidates)
	}

	sampled := make(map[int]bool)
	for i := 0; i < sampleSize; i++ {
		var idx int
		for {
			idx = rand.Intn(len(candidates))
			if !sampled[idx] {
				sampled[idx] = true
				break
			}
		}

		key := candidates[idx]
		if obj, exists := s.data[key]; exists {
			if obj.LRU < oldestLRU {
				oldestLRU = obj.LRU
				oldestKey = key
			}
		}
	}

	if oldestKey != "" {
		s.deleteInternal(oldestKey)
		return oldestKey
	}

	return ""
}

func (s *Store) evictRandom(volatileOnly bool) string {
	var candidates []string

	if volatileOnly {
		for key := range s.expires {
			if _, exists := s.data[key]; exists {
				candidates = append(candidates, key)
			}
		}
	} else {
		for key := range s.data {
			candidates = append(candidates, key)
		}
	}

	if len(candidates) == 0 {
		return ""
	}

	key := candidates[rand.Intn(len(candidates))]
	s.deleteInternal(key)
	return key
}

func (s *Store) evictShortestTTL() string {
	var shortestKey string
	var shortestTTL time.Duration = time.Duration(1<<63 - 1)

	now := time.Now()
	for key, expireTime := range s.expires {
		if _, exists := s.data[key]; exists {
			ttl := expireTime.Sub(now)
			if ttl < shortestTTL {
				shortestTTL = ttl
				shortestKey = key
			}
		}
	}

	if shortestKey != "" {
		s.deleteInternal(shortestKey)
		return shortestKey
	}

	return ""
}

func (s *Store) deleteInternal(key string) {
	if s.evictionConfig != nil && s.evictionConfig.memoryTracking {
		if obj, exists := s.data[key]; exists {
			s.evictionConfig.currentMemory -= EstimateObjectSize(obj)
			s.evictionConfig.currentMemory -= EstimateKeySize(key)
		}
	}

	delete(s.data, key)
	delete(s.expires, key)
	s.notifyKeyModified(key)
}

func (s *Store) UpdateMemoryUsage(delta int64) {
	if s.evictionConfig != nil && s.evictionConfig.memoryTracking {
		s.evictionConfig.currentMemory += delta
	}
}

func (s *Store) GetMemoryUsage() int64 {
	if s.evictionConfig != nil {
		return s.evictionConfig.currentMemory
	}
	return 0
}

func (s *Store) IsMemoryExceeded() bool {
	if s.evictionConfig == nil || s.evictionConfig.MaxMemory == 0 {
		return false
	}
	return s.evictionConfig.currentMemory > s.evictionConfig.MaxMemory
}

func (s *Store) TryEvictUntilUnderLimit() error {
	if !s.IsMemoryExceeded() {
		return nil
	}

	if s.evictionConfig.Policy == EvictionNoEviction {
		return fmt.Errorf("OOM command not allowed when used memory > 'maxmemory'")
	}

	maxAttempts := 10
	for i := 0; i < maxAttempts && s.IsMemoryExceeded(); i++ {
		evictedKey, ok := s.PerformEviction()
		if !ok || evictedKey == "" {

			return fmt.Errorf("OOM: no keys available for eviction")
		}
	}

	if s.IsMemoryExceeded() {
		return fmt.Errorf("OOM: unable to evict enough keys")
	}

	return nil
}
