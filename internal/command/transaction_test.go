package command

import (
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
)

func TestMultiCommand(t *testing.T) {
	multi := MultiCommand()

	result := multi([]resp.Value{})
	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected +OK, got %v", result)
	}

	result = multi([]resp.Value{
		{Type: resp.BulkString, Str: "arg"},
	})
	if result.Type != resp.Error {
		t.Errorf("Expected error for MULTI with arguments, got %v", result.Type)
	}
}

func TestExecCommand(t *testing.T) {
	exec := ExecCommand()

	result := exec([]resp.Value{})
	if result.Type != resp.SimpleString || result.Str != "EXEC" {
		t.Errorf("Expected +EXEC marker, got %v", result)
	}

	result = exec([]resp.Value{
		{Type: resp.BulkString, Str: "arg"},
	})
	if result.Type != resp.Error {
		t.Errorf("Expected error for EXEC with arguments, got %v", result.Type)
	}
}

func TestDiscardCommand(t *testing.T) {
	discard := DiscardCommand()

	result := discard([]resp.Value{})
	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected +OK, got %v", result)
	}

	result = discard([]resp.Value{
		{Type: resp.BulkString, Str: "arg"},
	})
	if result.Type != resp.Error {
		t.Errorf("Expected error for DISCARD with arguments, got %v", result.Type)
	}
}
