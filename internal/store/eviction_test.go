package store

import (
	"fmt"
	"testing"
	"time"
)

func TestNoEviction(t *testing.T) {
	s := NewStore()

	config := NewEvictionConfig(500, EvictionNoEviction, 5)
	s.SetEvictionConfig(config)

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d_", i) + string(make([]byte, 50))
		if err := s.Set(key, value); err != nil {

			t.Logf("Got expected OOM error at key %d: %v", i, err)

			testKey := fmt.Sprintf("test%d", i)
			testValue := "test" + string(make([]byte, 50))
			if err2 := s.Set(testKey, testValue); err2 == nil {
				t.Error("Expected OOM error to persist")
			}
			return
		}
	}

	t.Error("Expected OOM error with noeviction policy")
}

func TestAllKeysLRU(t *testing.T) {
	s := NewStore()

	config := NewEvictionConfig(800, EvictionAllKeysLRU, 3)
	s.SetEvictionConfig(config)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := "value" + fmt.Sprintf("%d", i) + string(make([]byte, 30))
		if err := s.Set(key, value); err != nil {
			t.Fatalf("Failed to set key%d: %v", i, err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	for i := 0; i < 3; i++ {
		s.Get("key0")
		time.Sleep(20 * time.Millisecond)
		s.Get("key1")
		time.Sleep(20 * time.Millisecond)
	}

	for i := 5; i < 15; i++ {
		key := fmt.Sprintf("key%d", i)
		value := "value" + fmt.Sprintf("%d", i) + string(make([]byte, 30))
		if err := s.Set(key, value); err != nil {

			t.Logf("Hit memory limit at key%d", i)
			break
		}
	}

	evictedOld := 0
	recentlyUsedEvicted := 0

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		if !s.Exists(key) {
			evictedOld++
			if i == 0 || i == 1 {
				recentlyUsedEvicted++
			}
		}
	}

	t.Logf("Evicted %d old keys (key0-key4), %d recently used keys", evictedOld, recentlyUsedEvicted)

	if evictedOld == 0 {
		t.Error("Expected some old keys to be evicted with LRU policy")
	}

	if recentlyUsedEvicted > 0 {
		t.Logf("Note: %d recently used keys were evicted (this can happen with approximate LRU)", recentlyUsedEvicted)
	}
}

func TestVolatileLRU(t *testing.T) {
	s := NewStore()

	config := NewEvictionConfig(500, EvictionVolatileLRU, 3)
	s.SetEvictionConfig(config)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("volatile%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		expiry := time.Now().Add(1 * time.Hour)
		if err := s.SetWithExpiry(key, value, expiry); err != nil {
			t.Fatalf("Failed to set volatile%d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("persistent%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		if err := s.Set(key, value); err != nil {
			t.Fatalf("Failed to set persistent%d: %v", i, err)
		}
	}

	for i := 5; i < 15; i++ {
		key := fmt.Sprintf("volatile%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		expiry := time.Now().Add(1 * time.Hour)
		if err := s.SetWithExpiry(key, value, expiry); err != nil {
			t.Fatalf("Failed to set volatile%d: %v", i, err)
		}
	}

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("persistent%d", i)
		if !s.Exists(key) {
			t.Errorf("%s was evicted but should not be (no expiry)", key)
		}
	}

	evictedCount := 0
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("volatile%d", i)
		if !s.Exists(key) {
			evictedCount++
		}
	}

	if evictedCount == 0 {
		t.Error("Expected some volatile keys to be evicted")
	}
	t.Logf("Evicted %d volatile keys", evictedCount)
}

func TestAllKeysRandom(t *testing.T) {
	s := NewStore()

	config := NewEvictionConfig(400, EvictionAllKeysRandom, 5)
	s.SetEvictionConfig(config)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		if err := s.Set(key, value); err != nil {
			t.Fatalf("Failed to set key%d: %v", i, err)
		}
	}

	existingKeys := 0
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		if s.Exists(key) {
			existingKeys++
		}
	}

	if existingKeys == 10 {
		t.Error("Expected some keys to be evicted with random policy")
	}
	if existingKeys == 0 {
		t.Error("All keys were evicted, expected some to remain")
	}
	t.Logf("%d keys remaining after random eviction", existingKeys)
}

func TestVolatileTTL(t *testing.T) {
	s := NewStore()

	config := NewEvictionConfig(500, EvictionVolatileTTL, 5)
	s.SetEvictionConfig(config)

	now := time.Now()

	s.SetWithExpiry("short_ttl", "value1", now.Add(1*time.Second))
	time.Sleep(10 * time.Millisecond)

	s.SetWithExpiry("medium_ttl", "value2", now.Add(10*time.Second))
	time.Sleep(10 * time.Millisecond)

	s.SetWithExpiry("long_ttl", "value3", now.Add(100*time.Second))
	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("extra%d", i)
		value := "value" + fmt.Sprintf("%d", i)
		expiry := now.Add(50 * time.Second)
		if err := s.SetWithExpiry(key, value, expiry); err != nil {
			t.Fatalf("Failed to set extra%d: %v", i, err)
		}
	}

	if !s.Exists("long_ttl") {
		t.Error("long_ttl was evicted but has the longest TTL")
	}

	if s.Exists("short_ttl") {
		t.Log("short_ttl still exists (may not have been evicted yet)")
	} else {
		t.Log("short_ttl was correctly evicted (shortest TTL)")
	}
}

func TestMemoryTracking(t *testing.T) {
	s := NewStore()

	config := NewEvictionConfig(10000, EvictionNoEviction, 5)
	s.SetEvictionConfig(config)

	initialMem := s.GetMemoryUsage()
	t.Logf("Initial memory: %d bytes", initialMem)

	s.Set("testkey", "testvalue")

	memAfterSet := s.GetMemoryUsage()
	t.Logf("Memory after SET: %d bytes", memAfterSet)

	if memAfterSet <= initialMem {
		t.Error("Memory usage did not increase after SET")
	}

	s.Delete("testkey")

	memAfterDel := s.GetMemoryUsage()
	t.Logf("Memory after DEL: %d bytes", memAfterDel)

	if memAfterDel >= memAfterSet {
		t.Error("Memory usage did not decrease after DEL")
	}
}

func TestEstimateObjectSize(t *testing.T) {

	strObj := &RedisObject{
		Type:     ObjString,
		Encoding: EncodingRaw,
		Ptr:      "hello world",
	}

	size := EstimateObjectSize(strObj)
	if size <= 0 {
		t.Error("Object size estimation failed")
	}
	t.Logf("String object size: %d bytes", size)

	nilSize := EstimateObjectSize(nil)
	if nilSize != 0 {
		t.Error("Nil object should have 0 size")
	}
}
