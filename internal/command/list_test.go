package command

import (
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestLPushCommand(t *testing.T) {
	s := store.NewStore()
	lpush := LPushCommand(s)
	lrange := LRangeCommand(s)

	tests := []struct {
		name          string
		args          []resp.Value
		expectedType  resp.Type
		expectedInt   int64
		expectedError bool
	}{
		{
			name: "push single element",
			args: []resp.Value{
				{Type: resp.BulkString, Str: "list1"},
				{Type: resp.BulkString, Str: "value1"},
			},
			expectedType: resp.Integer,
			expectedInt:  1,
		},
		{
			name: "push multiple elements",
			args: []resp.Value{
				{Type: resp.BulkString, Str: "list2"},
				{Type: resp.BulkString, Str: "value1"},
				{Type: resp.BulkString, Str: "value2"},
				{Type: resp.BulkString, Str: "value3"},
			},
			expectedType: resp.Integer,
			expectedInt:  3,
		},
		{
			name: "wrong number of arguments",
			args: []resp.Value{
				{Type: resp.BulkString, Str: "list3"},
			},
			expectedType:  resp.Error,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lpush(tt.args)

			if result.Type != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, result.Type)
			}

			if !tt.expectedError && result.Type == resp.Integer {
				if result.Int != tt.expectedInt {
					t.Errorf("Expected %d, got %d", tt.expectedInt, result.Int)
				}
			}
		})
	}

	rangeResult := lrange([]resp.Value{
		{Type: resp.BulkString, Str: "list2"},
		{Type: resp.BulkString, Str: "0"},
		{Type: resp.BulkString, Str: "-1"},
	})

	if rangeResult.Type != resp.Array {
		t.Fatalf("Expected array result")
	}

	expected := []string{"value3", "value2", "value1"}
	for i, val := range rangeResult.Array {
		if val.Str != expected[i] {
			t.Errorf("Expected %s at position %d, got %s", expected[i], i, val.Str)
		}
	}
}

func TestRPushCommand(t *testing.T) {
	s := store.NewStore()
	rpush := RPushCommand(s)
	lrange := LRangeCommand(s)

	tests := []struct {
		name          string
		args          []resp.Value
		expectedType  resp.Type
		expectedInt   int64
		expectedError bool
	}{
		{
			name: "push single element",
			args: []resp.Value{
				{Type: resp.BulkString, Str: "list1"},
				{Type: resp.BulkString, Str: "value1"},
			},
			expectedType: resp.Integer,
			expectedInt:  1,
		},
		{
			name: "push multiple elements",
			args: []resp.Value{
				{Type: resp.BulkString, Str: "list2"},
				{Type: resp.BulkString, Str: "value1"},
				{Type: resp.BulkString, Str: "value2"},
				{Type: resp.BulkString, Str: "value3"},
			},
			expectedType: resp.Integer,
			expectedInt:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rpush(tt.args)

			if result.Type != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, result.Type)
			}

			if !tt.expectedError && result.Type == resp.Integer {
				if result.Int != tt.expectedInt {
					t.Errorf("Expected %d, got %d", tt.expectedInt, result.Int)
				}
			}
		})
	}

	rangeResult := lrange([]resp.Value{
		{Type: resp.BulkString, Str: "list2"},
		{Type: resp.BulkString, Str: "0"},
		{Type: resp.BulkString, Str: "-1"},
	})

	if rangeResult.Type != resp.Array {
		t.Fatalf("Expected array result")
	}

	expected := []string{"value1", "value2", "value3"}
	for i, val := range rangeResult.Array {
		if val.Str != expected[i] {
			t.Errorf("Expected %s at position %d, got %s", expected[i], i, val.Str)
		}
	}
}

func TestLPopCommand(t *testing.T) {
	s := store.NewStore()
	rpush := RPushCommand(s)
	lpop := LPopCommand(s)

	rpush([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	})

	result := lpop([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
	})

	if result.Type != resp.BulkString {
		t.Fatalf("Expected bulk string result")
	}

	if result.Str != "a" {
		t.Errorf("Expected 'a', got %s", result.Str)
	}

	result = lpop([]resp.Value{
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Error("Expected null bulk string for nonexistent list")
	}

	result = lpop([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
		{Type: resp.BulkString, Str: "extra"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error for wrong number of arguments")
	}
}

func TestRPopCommand(t *testing.T) {
	s := store.NewStore()
	rpush := RPushCommand(s)
	rpop := RPopCommand(s)

	rpush([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	})

	result := rpop([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
	})

	if result.Type != resp.BulkString {
		t.Fatalf("Expected bulk string result")
	}

	if result.Str != "c" {
		t.Errorf("Expected 'c', got %s", result.Str)
	}

	result = rpop([]resp.Value{
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.BulkString || !result.Null {
		t.Error("Expected null bulk string for nonexistent list")
	}
}

func TestLLenCommand(t *testing.T) {
	s := store.NewStore()
	rpush := RPushCommand(s)
	llen := LLenCommand(s)

	result := llen([]resp.Value{
		{Type: resp.BulkString, Str: "nonexistent"},
	})

	if result.Type != resp.Integer {
		t.Fatalf("Expected integer result")
	}

	if result.Int != 0 {
		t.Errorf("Expected 0, got %d", result.Int)
	}

	rpush([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
	})

	result = llen([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
	})

	if result.Type != resp.Integer {
		t.Fatalf("Expected integer result")
	}

	if result.Int != 3 {
		t.Errorf("Expected 3, got %d", result.Int)
	}

	s.Set("stringkey", "value")
	result = llen([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error for wrong type")
	}
}

func TestLRangeCommand(t *testing.T) {
	s := store.NewStore()
	rpush := RPushCommand(s)
	lrange := LRangeCommand(s)

	rpush([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
		{Type: resp.BulkString, Str: "a"},
		{Type: resp.BulkString, Str: "b"},
		{Type: resp.BulkString, Str: "c"},
		{Type: resp.BulkString, Str: "d"},
		{Type: resp.BulkString, Str: "e"},
	})

	tests := []struct {
		name     string
		start    string
		stop     string
		expected []string
	}{
		{
			name:     "full range",
			start:    "0",
			stop:     "-1",
			expected: []string{"a", "b", "c", "d", "e"},
		},
		{
			name:     "partial range",
			start:    "1",
			stop:     "3",
			expected: []string{"b", "c", "d"},
		},
		{
			name:     "negative indices",
			start:    "-3",
			stop:     "-1",
			expected: []string{"c", "d", "e"},
		},
		{
			name:     "single element",
			start:    "2",
			stop:     "2",
			expected: []string{"c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := lrange([]resp.Value{
				{Type: resp.BulkString, Str: "list1"},
				{Type: resp.BulkString, Str: tt.start},
				{Type: resp.BulkString, Str: tt.stop},
			})

			if result.Type != resp.Array {
				t.Fatalf("Expected array result")
			}

			if len(result.Array) != len(tt.expected) {
				t.Fatalf("Expected %d elements, got %d", len(tt.expected), len(result.Array))
			}

			for i, val := range result.Array {
				if val.Str != tt.expected[i] {
					t.Errorf("Expected %s at position %d, got %s", tt.expected[i], i, val.Str)
				}
			}
		})
	}

	result := lrange([]resp.Value{
		{Type: resp.BulkString, Str: "nonexistent"},
		{Type: resp.BulkString, Str: "0"},
		{Type: resp.BulkString, Str: "-1"},
	})

	if result.Type != resp.Array {
		t.Fatalf("Expected array result")
	}

	if len(result.Array) != 0 {
		t.Errorf("Expected empty array, got %d elements", len(result.Array))
	}

	result = lrange([]resp.Value{
		{Type: resp.BulkString, Str: "list1"},
		{Type: resp.BulkString, Str: "invalid"},
		{Type: resp.BulkString, Str: "0"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error for invalid range")
	}
}

func TestListCommandsWrongType(t *testing.T) {
	s := store.NewStore()
	lpush := LPushCommand(s)
	rpush := RPushCommand(s)

	s.Set("stringkey", "value")

	result := lpush([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "value"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error when pushing to string key")
	}

	result = rpush([]resp.Value{
		{Type: resp.BulkString, Str: "stringkey"},
		{Type: resp.BulkString, Str: "value"},
	})

	if result.Type != resp.Error {
		t.Error("Expected error when pushing to string key")
	}
}
