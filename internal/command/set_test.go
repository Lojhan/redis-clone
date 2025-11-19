package command

import (
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestSAddCommand(t *testing.T) {
	s := store.NewStore()
	sadd := SAddCommand(s)

	result := sadd([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "member1"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1 member added, got %d", result.Int)
	}

	result = sadd([]resp.Value{
		{Type: resp.BulkString, Str: "myset2"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	})

	if result.Type != resp.Integer || result.Int != 3 {
		t.Errorf("Expected 3 members added, got %d", result.Int)
	}

	s.SAdd("myset3", "a")
	result = sadd([]resp.Value{
		{Type: resp.BulkString, Str: "myset3"},
		{Type: resp.BulkString, Str: "a"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 members added (already exists), got %d", result.Int)
	}

	s.Set("stringkey", "value")
	result = sadd([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error for wrong type, got %v", result.Type)
	}
}

func TestSRemCommand(t *testing.T) {
	s := store.NewStore()
	srem := SRemCommand(s)

	result := srem([]resp.Value{
		{Type: resp.BulkString, Str: "noset"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 members removed, got %d", result.Int)
	}

	s.SAdd("myset", "a", "b", "c")
	result = srem([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "a"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1 member removed, got %d", result.Int)
	}

	s.SAdd("myset2", "a", "b", "c")
	result = srem([]resp.Value{
		{Type: resp.BulkString, Str: "myset2"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2 members removed, got %d", result.Int)
	}

	s.Set("stringkey", "value")
	result = srem([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error for wrong type, got %v", result.Type)
	}
}

func TestSIsMemberCommand(t *testing.T) {
	s := store.NewStore()
	sismember := SIsMemberCommand(s)

	result := sismember([]resp.Value{
		{Type: resp.BulkString, Str: "noset"},
		{Type: resp.BulkString, Str: "member"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 (not a member), got %d", result.Int)
	}

	s.SAdd("myset", "a", "b", "c")
	result = sismember([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "a"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1 (is a member), got %d", result.Int)
	}

	result = sismember([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
		{Type: resp.BulkString, Str: "d"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 (not a member), got %d", result.Int)
	}
}

func TestSMembersCommand(t *testing.T) {
	s := store.NewStore()
	smembers := SMembersCommand(s)

	result := smembers([]resp.Value{
		{Type: resp.BulkString, Str: "noset"},
	})

	if result.Type != resp.Array || len(result.Array) != 0 {
		t.Errorf("Expected empty array, got %v with length %d", result.Type, len(result.Array))
	}

	s.SAdd("myset", "a", "b", "c")
	result = smembers([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	})

	if result.Type != resp.Array {
		t.Errorf("Expected array, got %v", result.Type)
	}

	if len(result.Array) != 3 {
		t.Errorf("Expected 3 members, got %d", len(result.Array))
	}

	members := make(map[string]bool)
	for _, v := range result.Array {
		members[v.Str] = true
	}

	for _, expected := range []string{"a", "b", "c"} {
		if !members[expected] {
			t.Errorf("Missing expected member %v", expected)
		}
	}
}

func TestSCardCommand(t *testing.T) {
	s := store.NewStore()
	scard := SCardCommand(s)

	result := scard([]resp.Value{
		{Type: resp.BulkString, Str: "noset"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 for non-existent set, got %d", result.Int)
	}

	s.SAdd("myset", "a", "b", "c")
	result = scard([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	})

	if result.Type != resp.Integer || result.Int != 3 {
		t.Errorf("Expected 3 members, got %d", result.Int)
	}

	s.Set("stringkey", "value")
	result = scard([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error for wrong type, got %v", result.Type)
	}
}

func TestSPopCommand(t *testing.T) {
	s := store.NewStore()
	spop := SPopCommand(s)

	result := spop([]resp.Value{
		{Type: resp.BulkString, Str: "noset"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Errorf("Expected null bulk string, got %v with Null=%v", result.Type, result.Null)
	}

	s.SAdd("myset", "a")
	result = spop([]resp.Value{
		{Type: resp.BulkString, Str: "myset"},
	})

	if result.Type != resp.BulkString || result.Null {
		t.Errorf("Expected bulk string, got %v with Null=%v", result.Type, result.Null)
	}

	if result.Str != "a" {
		t.Errorf("Expected 'a', got %v", result.Str)
	}

	card, _ := s.SCard("myset")
	if card != 0 {
		t.Errorf("Set should be empty after SPOP, card = %d", card)
	}

	s.SAdd("myset2", "a", "b", "c")
	result = spop([]resp.Value{
		{Type: resp.BulkString, Str: "myset2"},
	})

	if result.Type != resp.BulkString || result.Null {
		t.Errorf("Expected bulk string, got %v with Null=%v", result.Type, result.Null)
	}

	if s.SIsMember("myset2", result.Str) {
		t.Errorf("Popped member %v still in set", result.Str)
	}

	card, _ = s.SCard("myset2")
	if card != 2 {
		t.Errorf("Set should have 2 members after SPOP, card = %d", card)
	}
}
