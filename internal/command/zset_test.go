package command

import (
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestZAddCommand(t *testing.T) {
	s := store.NewStore()
	zadd := ZAddCommand(s)

	result := zadd([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "1.0"},
		{Type: resp.BulkString, Str: "one"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1 member added, got %d", result.Int)
	}

	result = zadd([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "2.0"},
		{Type: resp.BulkString, Str: "two"},
		{Type: resp.BulkString, Str: "3.0"},
		{Type: resp.BulkString, Str: "three"},
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2 members added, got %d", result.Int)
	}

	result = zadd([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "1.5"},
		{Type: resp.BulkString, Str: "one"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 members added (update), got %d", result.Int)
	}

	score, exists := s.ZScore("myzset", "one")
	if !exists || score != 1.5 {
		t.Errorf("Expected score 1.5, got %f (exists: %v)", score, exists)
	}

	s.Set("stringkey", "value")
	result = zadd([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "1.0"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error for wrong type, got %v", result.Type)
	}
}

func TestZRemCommand(t *testing.T) {
	s := store.NewStore()
	zrem := ZRemCommand(s)

	result := zrem([]resp.Value{
		{Type: resp.BulkString, Str: "nozset"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 members removed, got %d", result.Int)
	}

	s.ZAdd("myzset", 1.0, "one")
	s.ZAdd("myzset", 2.0, "two")
	s.ZAdd("myzset", 3.0, "three")

	result = zrem([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "one"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1 member removed, got %d", result.Int)
	}

	result = zrem([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "two"},
		{Type: resp.BulkString, Str: "three"},
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2 members removed, got %d", result.Int)
	}

	s.Set("stringkey", "value")
	result = zrem([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error for wrong type, got %v", result.Type)
	}
}

func TestZScoreCommand(t *testing.T) {
	s := store.NewStore()
	zscore := ZScoreCommand(s)

	result := zscore([]resp.Value{
		{Type: resp.BulkString, Str: "nozset"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Errorf("Expected null bulk string, got %v with Null=%v", result.Type, result.Null)
	}

	s.ZAdd("myzset", 1.5, "one")
	s.ZAdd("myzset", 2.5, "two")

	result = zscore([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "one"},
	})

	if result.Type != resp.BulkString || result.Str != "1.5" {
		t.Errorf("Expected score '1.5', got '%s'", result.Str)
	}

	result = zscore([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "three"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Errorf("Expected null bulk string for non-existent member")
	}
}

func TestZCardCommand(t *testing.T) {
	s := store.NewStore()
	zcard := ZCardCommand(s)

	result := zcard([]resp.Value{
		{Type: resp.BulkString, Str: "nozset"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 for non-existent zset, got %d", result.Int)
	}

	s.ZAdd("myzset", 1.0, "one")
	s.ZAdd("myzset", 2.0, "two")
	s.ZAdd("myzset", 3.0, "three")

	result = zcard([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
	})

	if result.Type != resp.Integer || result.Int != 3 {
		t.Errorf("Expected 3 members, got %d", result.Int)
	}

	s.Set("stringkey", "value")
	result = zcard([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error for wrong type, got %v", result.Type)
	}
}

func TestZRankCommand(t *testing.T) {
	s := store.NewStore()
	zrank := ZRankCommand(s)

	result := zrank([]resp.Value{
		{Type: resp.BulkString, Str: "nozset"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Errorf("Expected null bulk string, got %v with Null=%v", result.Type, result.Null)
	}

	s.ZAdd("myzset", 1.0, "one")
	s.ZAdd("myzset", 2.0, "two")
	s.ZAdd("myzset", 3.0, "three")

	result = zrank([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "one"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected rank 0, got %d", result.Int)
	}

	result = zrank([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "two"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected rank 1, got %d", result.Int)
	}

	result = zrank([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "three"},
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected rank 2, got %d", result.Int)
	}

	result = zrank([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "four"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Errorf("Expected null bulk string for non-existent member")
	}
}

func TestZRangeCommand(t *testing.T) {
	s := store.NewStore()
	zrange := ZRangeCommand(s)

	s.ZAdd("myzset", 1.0, "one")
	s.ZAdd("myzset", 2.0, "two")
	s.ZAdd("myzset", 3.0, "three")
	s.ZAdd("myzset", 4.0, "four")
	s.ZAdd("myzset", 5.0, "five")

	result := zrange([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "0"},
		{Type: resp.BulkString, Str: "2"},
	})

	if result.Type != resp.Array || len(result.Array) != 3 {
		t.Errorf("Expected array of 3 members, got %d", len(result.Array))
	}

	expected := []string{"one", "two", "three"}
	for i, v := range result.Array {
		if v.Str != expected[i] {
			t.Errorf("Expected member '%s' at index %d, got '%s'", expected[i], i, v.Str)
		}
	}

	result = zrange([]resp.Value{
		{Type: resp.BulkString, Str: "myzset"},
		{Type: resp.BulkString, Str: "1"},
		{Type: resp.BulkString, Str: "3"},
		{Type: resp.BulkString, Str: "WITHSCORES"},
	})

	if result.Type != resp.Array || len(result.Array) != 6 {
		t.Errorf("Expected array of 6 elements (3 members + scores), got %d", len(result.Array))
	}

	expectedWithScores := []string{"two", "2", "three", "3", "four", "4"}
	for i, v := range result.Array {
		if v.Str != expectedWithScores[i] {
			t.Errorf("Expected '%s' at index %d, got '%s'", expectedWithScores[i], i, v.Str)
		}
	}

	result = zrange([]resp.Value{
		{Type: resp.BulkString, Str: "nozset"},
		{Type: resp.BulkString, Str: "0"},
		{Type: resp.BulkString, Str: "10"},
	})

	if result.Type != resp.Array || len(result.Array) != 0 {
		t.Errorf("Expected empty array for non-existent zset, got %d elements", len(result.Array))
	}
}
