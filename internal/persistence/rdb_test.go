package persistence

import (
	"os"
	"testing"
	"time"

	"github.com/lojhan/redis-clone/internal/store"
)

func TestSaveLoadRDB(t *testing.T) {

	s := store.NewStore()

	s.Set("string_key", "hello world")
	s.Set("int_key", "12345")

	s.LPush("list_key", "a", "b", "c")

	s.HSet("hash_key", "field1", "value1")
	s.HSet("hash_key", "field2", "value2")

	s.SAdd("set_key", "member1", "member2", "member3")

	s.ZAdd("zset_key", 1.0, "member1")
	s.ZAdd("zset_key", 2.0, "member2")
	s.ZAdd("zset_key", 3.0, "member3")

	s.SetWithExpiry("expire_key", "will_expire", time.Now().Add(24*time.Hour))

	testFile := "test_dump.rdb"
	defer os.Remove(testFile)

	if err := SaveRDB(testFile, s); err != nil {
		t.Fatalf("Failed to save RDB: %v", err)
	}

	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Fatalf("RDB file was not created")
	}

	t.Log("RDB save successful, file size:", func() int64 {
		info, _ := os.Stat(testFile)
		return info.Size()
	}())

	s2 := store.NewStore()
	if err := LoadRDB(testFile, s2); err != nil {
		t.Fatalf("Failed to load RDB: %v", err)
	}

	if val, ok := s2.Get("string_key"); !ok || val != "hello world" {
		t.Errorf("string_key: got %v, want 'hello world'", val)
	}
	if val, ok := s2.Get("int_key"); !ok || val != "12345" {
		t.Errorf("int_key: got %v, want '12345'", val)
	}

	if list, err := s2.LRange("list_key", 0, -1); err != nil || len(list) != 3 {
		t.Errorf("list_key: got %v (len=%d), err=%v", list, len(list), err)
	}

	if val, ok := s2.HGet("hash_key", "field1"); !ok || val != "value1" {
		t.Errorf("hash field1: got %v, want 'value1'", val)
	}

	if members, err := s2.SMembers("set_key"); err != nil || len(members) != 3 {
		t.Errorf("set_key: got %d members, err=%v", len(members), err)
	}

	if score, ok := s2.ZScore("zset_key", "member1"); !ok || score != 1.0 {
		t.Errorf("zset member1: got %v, want 1.0", score)
	}

	if val, ok := s2.Get("expire_key"); !ok || val != "will_expire" {
		t.Errorf("expire_key: got %v, want 'will_expire'", val)
	}
}

func TestSaveEmptyStore(t *testing.T) {
	s := store.NewStore()

	testFile := "test_empty.rdb"
	defer os.Remove(testFile)

	if err := SaveRDB(testFile, s); err != nil {
		t.Fatalf("Failed to save empty RDB: %v", err)
	}

	info, err := os.Stat(testFile)
	if err != nil {
		t.Fatalf("RDB file was not created: %v", err)
	}

	if info.Size() == 0 {
		t.Errorf("RDB file is empty, expected header and EOF")
	}

	s2 := store.NewStore()
	if err := LoadRDB(testFile, s2); err != nil {
		t.Fatalf("Failed to load empty RDB: %v", err)
	}
}

func TestSnapshot(t *testing.T) {
	s := store.NewStore()

	s.Set("key1", "value1")
	s.Set("key2", "value2")
	s.LPush("list", "a", "b", "c")

	data, expires := s.Snapshot()

	if len(data) != 3 {
		t.Errorf("Expected 3 keys in snapshot, got %d", len(data))
	}

	if _, ok := data["key1"]; !ok {
		t.Error("key1 not found in snapshot")
	}
	if _, ok := data["key2"]; !ok {
		t.Error("key2 not found in snapshot")
	}
	if _, ok := data["list"]; !ok {
		t.Error("list not found in snapshot")
	}

	if len(expires) != 0 {
		t.Errorf("Expected no expiries, got %d", len(expires))
	}

	s.Set("key3", "value3")

	if len(data) != 3 {
		t.Errorf("Snapshot was modified, expected 3 keys, got %d", len(data))
	}
}

func TestLoadNonExistentRDB(t *testing.T) {
	s := store.NewStore()

	err := LoadRDB("non_existent.rdb", s)
	if err != nil {
		t.Errorf("Loading non-existent RDB should not error: %v", err)
	}
}
