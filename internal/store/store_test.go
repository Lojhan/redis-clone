package store

import (
	"testing"
	"time"
)

func TestStoreSetGet(t *testing.T) {
	store := NewStore()

	store.Set("key1", "value1")
	value, exists := store.Get("key1")

	if !exists {
		t.Error("Expected key1 to exist")
	}

	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	_, exists = store.Get("nonexistent")
	if exists {
		t.Error("Expected nonexistent key to not exist")
	}
}

func TestStoreEncodings(t *testing.T) {
	store := NewStore()

	tests := []struct {
		name             string
		value            string
		expectedEncoding ObjectEncoding
	}{
		{
			name:             "integer encoding",
			value:            "12345",
			expectedEncoding: EncodingInt,
		},
		{
			name:             "embstr encoding",
			value:            "short",
			expectedEncoding: EncodingEmbstr,
		},
		{
			name:             "raw encoding",
			value:            "this is a very long string that exceeds the embstr size limit",
			expectedEncoding: EncodingRaw,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store.Set(tt.name, tt.value)

			encoding, exists := store.GetEncoding(tt.name)
			if !exists {
				t.Fatalf("Key %s does not exist", tt.name)
			}

			if encoding != tt.expectedEncoding {
				t.Errorf("Expected encoding %v, got %v", tt.expectedEncoding, encoding)
			}

			value, exists := store.Get(tt.name)
			if !exists {
				t.Fatalf("Key %s does not exist", tt.name)
			}

			if value != tt.value {
				t.Errorf("Expected value %q, got %q", tt.value, value)
			}
		})
	}
}

func TestStoreExpiry(t *testing.T) {
	store := NewStore()

	pastTime := time.Now().Add(-1 * time.Second)
	store.SetWithExpiry("expired", "value", pastTime)

	_, exists := store.Get("expired")
	if exists {
		t.Error("Expected expired key to not exist")
	}

	futureTime := time.Now().Add(1 * time.Hour)
	store.SetWithExpiry("future", "value", futureTime)

	value, exists := store.Get("future")
	if !exists {
		t.Error("Expected future key to exist")
	}

	if value != "value" {
		t.Errorf("Expected value, got %s", value)
	}

	shortTime := time.Now().Add(100 * time.Millisecond)
	store.SetWithExpiry("short", "value", shortTime)

	_, exists = store.Get("short")
	if !exists {
		t.Error("Expected short key to exist initially")
	}

	time.Sleep(150 * time.Millisecond)

	_, exists = store.Get("short")
	if exists {
		t.Error("Expected short key to not exist after expiry")
	}
}

func TestStoreExists(t *testing.T) {
	store := NewStore()

	if store.Exists("key1") {
		t.Error("Expected key1 to not exist")
	}

	store.Set("key1", "value1")

	if !store.Exists("key1") {
		t.Error("Expected key1 to exist")
	}

	pastTime := time.Now().Add(-1 * time.Second)
	store.SetWithExpiry("expired", "value", pastTime)

	if store.Exists("expired") {
		t.Error("Expected expired key to not exist")
	}
}

func TestStoreDelete(t *testing.T) {
	store := NewStore()

	deleted := store.Delete("nonexistent")
	if deleted {
		t.Error("Expected delete of nonexistent key to return false")
	}

	store.Set("key1", "value1")
	deleted = store.Delete("key1")

	if !deleted {
		t.Error("Expected delete to return true")
	}

	if store.Exists("key1") {
		t.Error("Expected key1 to not exist after deletion")
	}
}

func TestStoreSetNX(t *testing.T) {
	store := NewStore()

	set, err := store.SetNX("key1", "value1")
	if err != nil {
		t.Fatalf("SetNX failed: %v", err)
	}
	if !set {
		t.Error("Expected SetNX to return true for non-existent key")
	}

	value, _ := store.Get("key1")
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	set, err = store.SetNX("key1", "value2")
	if err != nil {
		t.Fatalf("SetNX failed: %v", err)
	}
	if set {
		t.Error("Expected SetNX to return false for existing key")
	}

	value, _ = store.Get("key1")
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	pastTime := time.Now().Add(-1 * time.Second)
	store.SetWithExpiry("expired", "old", pastTime)

	set, err = store.SetNX("expired", "new")
	if err != nil {
		t.Fatalf("SetNX failed: %v", err)
	}
	if !set {
		t.Error("Expected SetNX to return true for expired key")
	}

	value, _ = store.Get("expired")
	if value != "new" {
		t.Errorf("Expected new, got %s", value)
	}
}

func TestStoreSetXX(t *testing.T) {
	store := NewStore()

	set, err := store.SetXX("key1", "value1")
	if err != nil {
		t.Fatalf("SetXX failed: %v", err)
	}
	if set {
		t.Error("Expected SetXX to return false for non-existent key")
	}

	if store.Exists("key1") {
		t.Error("Expected key1 to not exist")
	}

	store.Set("key1", "value1")

	set, err = store.SetXX("key1", "value2")
	if err != nil {
		t.Fatalf("SetXX failed: %v", err)
	}
	if !set {
		t.Error("Expected SetXX to return true for existing key")
	}

	value, _ := store.Get("key1")
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}

	pastTime := time.Now().Add(-1 * time.Second)
	store.SetWithExpiry("expired", "old", pastTime)

	set, err = store.SetXX("expired", "new")
	if err != nil {
		t.Fatalf("SetXX failed: %v", err)
	}
	if set {
		t.Error("Expected SetXX to return false for expired key")
	}
}

func TestStoreGetType(t *testing.T) {
	store := NewStore()

	_, exists := store.GetType("nonexistent")
	if exists {
		t.Error("Expected GetType to return false for nonexistent key")
	}

	store.Set("string", "value")
	objType, exists := store.GetType("string")

	if !exists {
		t.Error("Expected GetType to return true for existing key")
	}

	if objType != ObjString {
		t.Errorf("Expected ObjString type, got %v", objType)
	}
}

func TestStoreKeys(t *testing.T) {
	store := NewStore()

	keys := store.Keys()
	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}

	store.Set("key1", "value1")
	store.Set("key2", "value2")
	store.Set("key3", "value3")

	keys = store.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}

	pastTime := time.Now().Add(-1 * time.Second)
	store.SetWithExpiry("expired", "value", pastTime)

	keys = store.Keys()
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys (excluding expired), got %d", len(keys))
	}
}

func TestStoreSize(t *testing.T) {
	store := NewStore()

	if store.Size() != 0 {
		t.Errorf("Expected size 0, got %d", store.Size())
	}

	store.Set("key1", "value1")
	store.Set("key2", "value2")

	if store.Size() != 2 {
		t.Errorf("Expected size 2, got %d", store.Size())
	}

	store.Delete("key1")

	if store.Size() != 1 {
		t.Errorf("Expected size 1, got %d", store.Size())
	}

	pastTime := time.Now().Add(-1 * time.Second)
	store.SetWithExpiry("expired", "value", pastTime)

	if store.Size() != 1 {
		t.Errorf("Expected size 1 (excluding expired), got %d", store.Size())
	}
}

func TestStoreConcurrency(t *testing.T) {
	store := NewStore()

	done := make(chan bool)

	go func() {
		for range 100 {
			store.Set("key", "value")
		}
		done <- true
	}()

	go func() {
		for range 100 {
			store.Get("key")
		}
		done <- true
	}()

	<-done
	<-done

}

func TestStoreLPush(t *testing.T) {
	store := NewStore()

	length, err := store.LPush("list1", "value1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 1 {
		t.Errorf("Expected length 1, got %d", length)
	}

	length, err = store.LPush("list1", "value2", "value3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}

	values, err := store.LRange("list1", 0, -1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expected := []string{"value3", "value2", "value1"}
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, got %d", len(expected), len(values))
	}
	for i, v := range values {
		if v != expected[i] {
			t.Errorf("Expected value[%d] = %s, got %s", i, expected[i], v)
		}
	}
}

func TestStoreRPush(t *testing.T) {
	store := NewStore()

	length, err := store.RPush("list1", "value1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 1 {
		t.Errorf("Expected length 1, got %d", length)
	}

	length, err = store.RPush("list1", "value2", "value3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}

	values, err := store.LRange("list1", 0, -1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expected := []string{"value1", "value2", "value3"}
	if len(values) != len(expected) {
		t.Fatalf("Expected %d values, got %d", len(expected), len(values))
	}
	for i, v := range values {
		if v != expected[i] {
			t.Errorf("Expected value[%d] = %s, got %s", i, expected[i], v)
		}
	}
}

func TestStoreLPop(t *testing.T) {
	store := NewStore()

	value, ok := store.LPop("nonexistent")
	if ok {
		t.Error("Expected false for nonexistent list")
	}

	store.RPush("list1", "value1", "value2", "value3")

	value, ok = store.LPop("list1")
	if !ok {
		t.Error("Expected true for successful pop")
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	length, _ := store.LLen("list1")
	if length != 2 {
		t.Errorf("Expected length 2, got %d", length)
	}

	store.LPop("list1")
	store.LPop("list1")

	if store.Exists("list1") {
		t.Error("Expected list to be deleted after popping all elements")
	}
}

func TestStoreRPop(t *testing.T) {
	store := NewStore()

	value, ok := store.RPop("nonexistent")
	if ok {
		t.Error("Expected false for nonexistent list")
	}

	store.RPush("list1", "value1", "value2", "value3")

	value, ok = store.RPop("list1")
	if !ok {
		t.Error("Expected true for successful pop")
	}
	if value != "value3" {
		t.Errorf("Expected value3, got %s", value)
	}

	length, _ := store.LLen("list1")
	if length != 2 {
		t.Errorf("Expected length 2, got %d", length)
	}
}

func TestStoreLLen(t *testing.T) {
	store := NewStore()

	length, err := store.LLen("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected length 0, got %d", length)
	}

	store.RPush("list1", "a", "b", "c")
	length, err = store.LLen("list1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}
}

func TestStoreLRange(t *testing.T) {
	store := NewStore()

	store.RPush("list1", "a", "b", "c", "d", "e")

	tests := []struct {
		name     string
		start    int64
		stop     int64
		expected []string
	}{
		{
			name:     "full range",
			start:    0,
			stop:     -1,
			expected: []string{"a", "b", "c", "d", "e"},
		},
		{
			name:     "partial range",
			start:    1,
			stop:     3,
			expected: []string{"b", "c", "d"},
		},
		{
			name:     "negative indices",
			start:    -3,
			stop:     -1,
			expected: []string{"c", "d", "e"},
		},
		{
			name:     "single element",
			start:    2,
			stop:     2,
			expected: []string{"c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := store.LRange("list1", tt.start, tt.stop)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(values) != len(tt.expected) {
				t.Fatalf("Expected %d values, got %d", len(tt.expected), len(values))
			}

			for i, v := range values {
				if v != tt.expected[i] {
					t.Errorf("Expected value[%d] = %s, got %s", i, tt.expected[i], v)
				}
			}
		})
	}

	values, err := store.LRange("nonexistent", 0, -1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(values) != 0 {
		t.Errorf("Expected empty slice, got %d elements", len(values))
	}
}

func TestStoreListWrongType(t *testing.T) {
	store := NewStore()

	store.Set("stringkey", "value")

	_, err := store.LPush("stringkey", "value")
	if err == nil {
		t.Error("Expected error when pushing to string key")
	}
	if err.Error() != "WRONGTYPE Operation against a key holding the wrong kind of value" {
		t.Errorf("Unexpected error message: %v", err)
	}

	length, err := store.LLen("stringkey")
	if err == nil {
		t.Error("Expected error when getting length of string key")
	}
	if length != 0 {
		t.Errorf("Expected length 0 on error, got %d", length)
	}
}

func TestStoreHSet(t *testing.T) {
	store := NewStore()

	count, err := store.HSet("hash1", "field1", "value1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count 1 for new field, got %d", count)
	}

	count, err = store.HSet("hash1", "field1", "value2")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0 for updated field, got %d", count)
	}

	count, err = store.HSet("hash1", "field2", "value3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count 1 for new field, got %d", count)
	}

	length, _ := store.HLen("hash1")
	if length != 2 {
		t.Errorf("Expected hash length 2, got %d", length)
	}
}

func TestStoreHGet(t *testing.T) {
	store := NewStore()

	value, exists := store.HGet("nonexistent", "field1")
	if exists {
		t.Error("Expected field to not exist in non-existent hash")
	}

	store.HSet("hash1", "field1", "value1")
	store.HSet("hash1", "field2", "value2")

	value, exists = store.HGet("hash1", "field1")
	if !exists {
		t.Error("Expected field1 to exist")
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	_, exists = store.HGet("hash1", "nonexistent")
	if exists {
		t.Error("Expected nonexistent field to not exist")
	}
}

func TestStoreHDel(t *testing.T) {
	store := NewStore()

	count, err := store.HDel("nonexistent", "field1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count 0, got %d", count)
	}

	store.HSet("hash1", "field1", "value1")
	store.HSet("hash1", "field2", "value2")
	store.HSet("hash1", "field3", "value3")

	count, err = store.HDel("hash1", "field1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count 1, got %d", count)
	}

	if store.HExists("hash1", "field1") {
		t.Error("Expected field1 to be deleted")
	}

	count, err = store.HDel("hash1", "field2", "field3", "nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected count 2, got %d", count)
	}

	if store.Exists("hash1") {
		t.Error("Expected hash1 to be deleted when all fields removed")
	}
}

func TestStoreHExists(t *testing.T) {
	store := NewStore()

	if store.HExists("nonexistent", "field1") {
		t.Error("Expected field to not exist in non-existent hash")
	}

	store.HSet("hash1", "field1", "value1")

	if !store.HExists("hash1", "field1") {
		t.Error("Expected field1 to exist")
	}

	if store.HExists("hash1", "field2") {
		t.Error("Expected field2 to not exist")
	}
}

func TestStoreHLen(t *testing.T) {
	store := NewStore()

	length, err := store.HLen("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 0 {
		t.Errorf("Expected length 0, got %d", length)
	}

	store.HSet("hash1", "field1", "value1")
	store.HSet("hash1", "field2", "value2")
	store.HSet("hash1", "field3", "value3")

	length, err = store.HLen("hash1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}

	store.Set("stringkey", "value")
	length, err = store.HLen("stringkey")
	if err == nil {
		t.Error("Expected error for wrong type")
	}
}

func TestStoreHGetAll(t *testing.T) {
	store := NewStore()

	all, err := store.HGetAll("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("Expected empty map, got %d entries", len(all))
	}

	store.HSet("hash1", "field1", "value1")
	store.HSet("hash1", "field2", "value2")
	store.HSet("hash1", "field3", "value3")

	all, err = store.HGetAll("hash1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(all) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(all))
	}

	expected := map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}

	for field, expectedValue := range expected {
		value, exists := all[field]
		if !exists {
			t.Errorf("Expected field %s to exist", field)
		}
		if value != expectedValue {
			t.Errorf("Expected %s for field %s, got %s", expectedValue, field, value)
		}
	}
}

func TestStoreHashWrongType(t *testing.T) {
	store := NewStore()

	store.Set("stringkey", "value")

	_, err := store.HSet("stringkey", "field", "value")
	if err == nil {
		t.Error("Expected error when setting hash field on string key")
	}

	_, err = store.HDel("stringkey", "field")
	if err == nil {
		t.Error("Expected error when deleting hash field on string key")
	}
}

func TestStoreSAdd(t *testing.T) {
	store := NewStore()

	added, err := store.SAdd("set1", "member1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if added != 1 {
		t.Errorf("Expected 1 member added, got %d", added)
	}

	added, err = store.SAdd("set1", "member1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if added != 0 {
		t.Errorf("Expected 0 members added (already exists), got %d", added)
	}

	added, err = store.SAdd("set2", "a", "b", "c")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if added != 3 {
		t.Errorf("Expected 3 members added, got %d", added)
	}

	added, err = store.SAdd("set2", "b", "c", "d")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if added != 1 {
		t.Errorf("Expected 1 member added (d is new), got %d", added)
	}

	store.Set("stringkey", "value")
	_, err = store.SAdd("stringkey", "member")
	if err == nil {
		t.Error("Expected error when adding to non-set key")
	}
}

func TestStoreSRem(t *testing.T) {
	store := NewStore()

	removed, err := store.SRem("noset", "member")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if removed != 0 {
		t.Errorf("Expected 0 members removed, got %d", removed)
	}

	store.SAdd("set1", "member1", "member2", "member3")
	removed, err = store.SRem("set1", "member1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if removed != 1 {
		t.Errorf("Expected 1 member removed, got %d", removed)
	}

	removed, err = store.SRem("set1", "member2", "member3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if removed != 2 {
		t.Errorf("Expected 2 members removed, got %d", removed)
	}

	if store.Exists("set1") {
		t.Error("Expected set1 to be removed after all members deleted")
	}

	store.Set("stringkey", "value")
	_, err = store.SRem("stringkey", "member")
	if err == nil {
		t.Error("Expected error when removing from non-set key")
	}
}

func TestStoreSIsMember(t *testing.T) {
	store := NewStore()

	if store.SIsMember("noset", "member") {
		t.Error("Expected false for non-existent set")
	}

	store.SAdd("set1", "a", "b", "c")

	if !store.SIsMember("set1", "a") {
		t.Error("Expected true for existing member")
	}

	if store.SIsMember("set1", "d") {
		t.Error("Expected false for non-existing member")
	}
}

func TestStoreSMembers(t *testing.T) {
	store := NewStore()

	members, err := store.SMembers("noset")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected empty slice, got %d members", len(members))
	}

	store.SAdd("set1", "a", "b", "c")
	members, err = store.SMembers("set1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	memberMap := make(map[string]bool)
	for _, m := range members {
		memberMap[m] = true
	}

	for _, expected := range []string{"a", "b", "c"} {
		if !memberMap[expected] {
			t.Errorf("Expected member %s not found", expected)
		}
	}
}

func TestStoreSCard(t *testing.T) {
	store := NewStore()

	card, err := store.SCard("noset")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if card != 0 {
		t.Errorf("Expected 0 for non-existent set, got %d", card)
	}

	store.SAdd("set1", "a", "b", "c")
	card, err = store.SCard("set1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if card != 3 {
		t.Errorf("Expected 3 members, got %d", card)
	}

	store.Set("stringkey", "value")
	_, err = store.SCard("stringkey")
	if err == nil {
		t.Error("Expected error for wrong type")
	}
}

func TestStoreSPop(t *testing.T) {
	store := NewStore()

	member, ok := store.SPop("noset")
	if ok {
		t.Error("Expected false for non-existent set")
	}
	if member != "" {
		t.Errorf("Expected empty string, got %s", member)
	}

	store.SAdd("set1", "a", "b", "c")
	member, ok = store.SPop("set1")
	if !ok {
		t.Error("Expected true for successful pop")
	}
	if member != "a" && member != "b" && member != "c" {
		t.Errorf("Expected one of a/b/c, got %s", member)
	}

	if store.SIsMember("set1", member) {
		t.Errorf("Member %s should have been removed", member)
	}

	card, _ := store.SCard("set1")
	if card != 2 {
		t.Errorf("Expected 2 members remaining, got %d", card)
	}

	store.SPop("set1")
	store.SPop("set1")

	if store.Exists("set1") {
		t.Error("Expected set to be removed after all members popped")
	}
}

func TestStoreSetWrongType(t *testing.T) {
	store := NewStore()

	store.Set("stringkey", "value")

	_, err := store.SAdd("stringkey", "member")
	if err == nil {
		t.Error("Expected error when adding to non-set key")
	}

	_, err = store.SRem("stringkey", "member")
	if err == nil {
		t.Error("Expected error when removing from non-set key")
	}

	_, err = store.SCard("stringkey")
	if err == nil {
		t.Error("Expected error when getting cardinality of non-set key")
	}
}

func TestStoreZAdd(t *testing.T) {
	store := NewStore()

	added, err := store.ZAdd("zset1", 1.0, "one")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if added != 1 {
		t.Errorf("Expected 1 member added, got %d", added)
	}

	added, err = store.ZAdd("zset1", 2.0, "one")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if added != 0 {
		t.Errorf("Expected 0 members added (update), got %d", added)
	}

	score, exists := store.ZScore("zset1", "one")
	if !exists || score != 2.0 {
		t.Errorf("Expected score 2.0, got %f (exists: %v)", score, exists)
	}

	store.Set("stringkey", "value")
	_, err = store.ZAdd("stringkey", 1.0, "member")
	if err == nil {
		t.Error("Expected error when adding to non-zset key")
	}
}

func TestStoreZRem(t *testing.T) {
	store := NewStore()

	removed, err := store.ZRem("nozset", "member")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if removed != 0 {
		t.Errorf("Expected 0 members removed, got %d", removed)
	}

	store.ZAdd("zset1", 1.0, "one")
	store.ZAdd("zset1", 2.0, "two")
	store.ZAdd("zset1", 3.0, "three")

	removed, err = store.ZRem("zset1", "one")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if removed != 1 {
		t.Errorf("Expected 1 member removed, got %d", removed)
	}

	_, exists := store.ZScore("zset1", "one")
	if exists {
		t.Error("Expected member 'one' to be removed")
	}

	store.Set("stringkey", "value")
	_, err = store.ZRem("stringkey", "member")
	if err == nil {
		t.Error("Expected error when removing from non-zset key")
	}
}

func TestStoreZScore(t *testing.T) {
	store := NewStore()

	_, exists := store.ZScore("nozset", "member")
	if exists {
		t.Error("Expected false for non-existent zset")
	}

	store.ZAdd("zset1", 1.5, "one")
	store.ZAdd("zset1", 2.5, "two")

	score, exists := store.ZScore("zset1", "one")
	if !exists || score != 1.5 {
		t.Errorf("Expected score 1.5, got %f (exists: %v)", score, exists)
	}

	_, exists = store.ZScore("zset1", "three")
	if exists {
		t.Error("Expected false for non-existent member")
	}
}

func TestStoreZCard(t *testing.T) {
	store := NewStore()

	card, err := store.ZCard("nozset")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if card != 0 {
		t.Errorf("Expected 0 for non-existent zset, got %d", card)
	}

	store.ZAdd("zset1", 1.0, "one")
	store.ZAdd("zset1", 2.0, "two")
	store.ZAdd("zset1", 3.0, "three")

	card, err = store.ZCard("zset1")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if card != 3 {
		t.Errorf("Expected 3 members, got %d", card)
	}

	store.Set("stringkey", "value")
	_, err = store.ZCard("stringkey")
	if err == nil {
		t.Error("Expected error for wrong type")
	}
}

func TestStoreZRank(t *testing.T) {
	store := NewStore()

	_, exists := store.ZRank("nozset", "member")
	if exists {
		t.Error("Expected false for non-existent zset")
	}

	store.ZAdd("zset1", 1.0, "one")
	store.ZAdd("zset1", 2.0, "two")
	store.ZAdd("zset1", 3.0, "three")

	rank, exists := store.ZRank("zset1", "one")
	if !exists || rank != 0 {
		t.Errorf("Expected rank 0, got %d (exists: %v)", rank, exists)
	}

	rank, exists = store.ZRank("zset1", "two")
	if !exists || rank != 1 {
		t.Errorf("Expected rank 1, got %d (exists: %v)", rank, exists)
	}

	rank, exists = store.ZRank("zset1", "three")
	if !exists || rank != 2 {
		t.Errorf("Expected rank 2, got %d (exists: %v)", rank, exists)
	}

	_, exists = store.ZRank("zset1", "four")
	if exists {
		t.Error("Expected false for non-existent member")
	}
}

func TestStoreZRange(t *testing.T) {
	store := NewStore()

	members, err := store.ZRange("nozset", 0, 10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("Expected empty slice, got %d members", len(members))
	}

	store.ZAdd("zset1", 1.0, "one")
	store.ZAdd("zset1", 2.0, "two")
	store.ZAdd("zset1", 3.0, "three")
	store.ZAdd("zset1", 4.0, "four")
	store.ZAdd("zset1", 5.0, "five")

	members, err = store.ZRange("zset1", 0, 2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	expected := []string{"one", "two", "three"}
	for i, zm := range members {
		if zm.Member != expected[i] {
			t.Errorf("Expected member '%s' at index %d, got '%s'", expected[i], i, zm.Member)
		}
	}
}

func TestStoreZSetWrongType(t *testing.T) {
	store := NewStore()

	store.Set("stringkey", "value")

	_, err := store.ZAdd("stringkey", 1.0, "member")
	if err == nil {
		t.Error("Expected error when adding to non-zset key")
	}

	_, err = store.ZRem("stringkey", "member")
	if err == nil {
		t.Error("Expected error when removing from non-zset key")
	}

	_, err = store.ZCard("stringkey")
	if err == nil {
		t.Error("Expected error when getting cardinality of non-zset key")
	}
}
