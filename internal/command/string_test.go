package command

import (
	"testing"
	"time"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestSetGetCommands(t *testing.T) {
	s := store.NewStore()
	setCmd := SetCommand(s)
	getCmd := GetCommand(s)

	result := setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value1"),
	})

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected OK, got %+v", result)
	}

	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Type != resp.BulkString || result.Str != "value1" {
		t.Errorf("Expected value1, got %+v", result)
	}

	result = getCmd([]resp.Value{
		resp.BulkStringValue("nonexistent"),
	})

	if !result.Null {
		t.Errorf("Expected null, got %+v", result)
	}
}

func TestSetNXOption(t *testing.T) {
	s := store.NewStore()
	setCmd := SetCommand(s)

	result := setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value1"),
		resp.BulkStringValue("NX"),
	})

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected OK, got %+v", result)
	}

	result = setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value2"),
		resp.BulkStringValue("NX"),
	})

	if !result.Null {
		t.Errorf("Expected null, got %+v", result)
	}

	getCmd := GetCommand(s)
	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Str != "value1" {
		t.Errorf("Expected value1, got %s", result.Str)
	}
}

func TestSetXXOption(t *testing.T) {
	s := store.NewStore()
	setCmd := SetCommand(s)

	result := setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value1"),
		resp.BulkStringValue("XX"),
	})

	if !result.Null {
		t.Errorf("Expected null, got %+v", result)
	}

	s.Set("key1", "value1")

	result = setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value2"),
		resp.BulkStringValue("XX"),
	})

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected OK, got %+v", result)
	}

	getCmd := GetCommand(s)
	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Str != "value2" {
		t.Errorf("Expected value2, got %s", result.Str)
	}
}

func TestSetEXOption(t *testing.T) {
	s := store.NewStore()
	setCmd := SetCommand(s)
	getCmd := GetCommand(s)

	result := setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value1"),
		resp.BulkStringValue("EX"),
		resp.BulkStringValue("1"),
	})

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected OK, got %+v", result)
	}

	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Str != "value1" {
		t.Errorf("Expected value1, got %s", result.Str)
	}

	time.Sleep(1100 * time.Millisecond)

	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if !result.Null {
		t.Errorf("Expected null after expiry, got %+v", result)
	}
}

func TestSetPXOption(t *testing.T) {
	s := store.NewStore()
	setCmd := SetCommand(s)
	getCmd := GetCommand(s)

	result := setCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("value1"),
		resp.BulkStringValue("PX"),
		resp.BulkStringValue("100"),
	})

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected OK, got %+v", result)
	}

	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Str != "value1" {
		t.Errorf("Expected value1, got %s", result.Str)
	}

	time.Sleep(150 * time.Millisecond)

	result = getCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if !result.Null {
		t.Errorf("Expected null after expiry, got %+v", result)
	}
}

func TestDelCommand(t *testing.T) {
	s := store.NewStore()
	delCmd := DelCommand(s)

	s.Set("key1", "value1")
	s.Set("key2", "value2")
	s.Set("key3", "value3")

	result := delCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1, got %+v", result)
	}

	result = delCmd([]resp.Value{
		resp.BulkStringValue("key2"),
		resp.BulkStringValue("key3"),
		resp.BulkStringValue("nonexistent"),
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2, got %+v", result)
	}
}

func TestExistsCommand(t *testing.T) {
	s := store.NewStore()
	existsCmd := ExistsCommand(s)

	result := existsCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Type != resp.Integer || result.Int != 0 {
		t.Errorf("Expected 0, got %+v", result)
	}

	s.Set("key1", "value1")
	s.Set("key2", "value2")

	result = existsCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1, got %+v", result)
	}

	result = existsCmd([]resp.Value{
		resp.BulkStringValue("key1"),
		resp.BulkStringValue("key2"),
		resp.BulkStringValue("nonexistent"),
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2, got %+v", result)
	}
}

func TestTypeCommand(t *testing.T) {
	s := store.NewStore()
	typeCmd := TypeCommand(s)

	result := typeCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Type != resp.SimpleString || result.Str != "none" {
		t.Errorf("Expected none, got %+v", result)
	}

	s.Set("key1", "value1")
	result = typeCmd([]resp.Value{
		resp.BulkStringValue("key1"),
	})

	if result.Type != resp.SimpleString || result.Str != "string" {
		t.Errorf("Expected string, got %+v", result)
	}
}

func TestIncrCommand(t *testing.T) {
	s := store.NewStore()
	incrCmd := IncrCommand(s)

	result := incrCmd([]resp.Value{
		resp.BulkStringValue("counter"),
	})

	if result.Type != resp.Integer || result.Int != 1 {
		t.Errorf("Expected 1, got %+v", result)
	}

	result = incrCmd([]resp.Value{
		resp.BulkStringValue("counter"),
	})

	if result.Type != resp.Integer || result.Int != 2 {
		t.Errorf("Expected 2, got %+v", result)
	}

	s.Set("string", "notanumber")
	result = incrCmd([]resp.Value{
		resp.BulkStringValue("string"),
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error, got %+v", result)
	}
}

func TestDecrCommand(t *testing.T) {
	s := store.NewStore()
	decrCmd := DecrCommand(s)

	result := decrCmd([]resp.Value{
		resp.BulkStringValue("counter"),
	})

	if result.Type != resp.Integer || result.Int != -1 {
		t.Errorf("Expected -1, got %+v", result)
	}

	result = decrCmd([]resp.Value{
		resp.BulkStringValue("counter"),
	})

	if result.Type != resp.Integer || result.Int != -2 {
		t.Errorf("Expected -2, got %+v", result)
	}

	s.Set("string", "notanumber")
	result = decrCmd([]resp.Value{
		resp.BulkStringValue("string"),
	})

	if result.Type != resp.Error {
		t.Errorf("Expected error, got %+v", result)
	}
}
