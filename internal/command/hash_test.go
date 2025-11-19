package command

import (
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestHSetCommand(t *testing.T) {
	s := store.NewStore()
	hset := HSetCommand(s)

	result := hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1 field added, got %d", result.Int)
	}

	result = hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field2"},
		{Type: resp.BulkString, Str: "value2"},
		{Type: resp.BulkString, Str: "field3"},
		{Type: resp.BulkString, Str: "value3"},
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2 fields added, got %d", result.Int)
	}

	result = hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "newvalue"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0 fields added (update), got %d", result.Int)
	}
}

func TestHGetCommand(t *testing.T) {
	s := store.NewStore()
	hset := HSetCommand(s)
	hget := HGetCommand(s)

	hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
	})

	result := hget([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
	})

	if result.Type != resp.BulkString || result.Str != "value1" {
		t.Errorf("Expected value1, got %s", result.Str)
	}

	result = hget([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Error("Expected null bulk string for non-existent field")
	}
}

func TestHDelCommand(t *testing.T) {
	s := store.NewStore()
	hset := HSetCommand(s)
	hdel := HDelCommand(s)

	hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "field2"},
		{Type: resp.BulkString, Str: "value2"},
		{Type: resp.BulkString, Str: "field3"},
		{Type: resp.BulkString, Str: "value3"},
	})

	result := hdel([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "field2"},
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2 fields deleted, got %d", result.Int)
	}
}

func TestHExistsCommand(t *testing.T) {
	s := store.NewStore()
	hset := HSetCommand(s)
	hexists := HExistsCommand(s)

	hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
	})

	result := hexists([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Error("Expected field to exist")
	}

	result = hexists([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Error("Expected field to not exist")
	}
}

func TestHLenCommand(t *testing.T) {
	s := store.NewStore()
	hset := HSetCommand(s)
	hlen := HLenCommand(s)

	result := hlen([]resp.Value{
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected length 0, got %d", result.Int)
	}

	hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "field2"},
		{Type: resp.BulkString, Str: "value2"},
		{Type: resp.BulkString, Str: "field3"},
		{Type: resp.BulkString, Str: "value3"},
	})

	result = hlen([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
	})

	if result.Type != resp.Integer || result.Int != 3 {
		t.Errorf("Expected length 3, got %d", result.Int)
	}
}

func TestHGetAllCommand(t *testing.T) {
	s := store.NewStore()
	hset := HSetCommand(s)
	hgetall := HGetAllCommand(s)

	hset([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
		{Type: resp.BulkString, Str: "field1"},
		{Type: resp.BulkString, Str: "value1"},
		{Type: resp.BulkString, Str: "field2"},
		{Type: resp.BulkString, Str: "value2"},
	})

	result := hgetall([]resp.Value{
		{Type: resp.BulkString, Str: "hash1"},
	})

	if result.Type != resp.Array {
		t.Fatal("Expected array result")
	}

	if len(result.Array) != 4 {
		t.Errorf("Expected 4 elements (2 field-value pairs), got %d", len(result.Array))
	}

	fieldValues := make(map[string]string)
	for i := 0; i < len(result.Array); i += 2 {
		field := result.Array[i].Str
		value := result.Array[i+1].Str
		fieldValues[field] = value
	}

	expected := map[string]string{
		"field1": "value1",
		"field2": "value2",
	}

	for field, expectedValue := range expected {
		if value, ok := fieldValues[field]; !ok || value != expectedValue {
			t.Errorf("Expected %s=%s, got %s=%s", field, expectedValue, field, value)
		}
	}
}

func TestHashCommandsWrongType(t *testing.T) {
	s := store.NewStore()
	s.Set("stringkey", "value")

	hset := HSetCommand(s)
	hdel := HDelCommand(s)

	result := hset([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "field"},
		{Type: resp.BulkString, Str: "value"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error when using HSET on string key")
	}

	result = hdel([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "field"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error when using HDEL on string key")
	}
}
