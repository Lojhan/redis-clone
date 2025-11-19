package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/lojhan/redis-clone/internal/command"
	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestTransactionMultiExec(t *testing.T) {
	server := NewServer()
	s := store.NewStore()

	server.RegisterCommand("SET", command.SetCommand(s))
	server.RegisterCommand("GET", command.GetCommand(s))

	go server.Start("16379")
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "localhost:16379")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	parser := resp.NewParser(bufio.NewReader(conn))
	serializer := resp.NewSerializer(writer)

	multi := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "MULTI"},
		},
	}
	serializer.Serialize(multi)
	writer.Flush()

	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse MULTI response: %v", err)
	}
	if response.Type != resp.SimpleString || response.Str != "OK" {
		t.Errorf("Expected +OK for MULTI, got %v", response)
	}

	setCmd := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "value1"},
		},
	}
	serializer.Serialize(setCmd)
	writer.Flush()

	response, err = parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SET response: %v", err)
	}
	if response.Type != resp.SimpleString || response.Str != "QUEUED" {
		t.Errorf("Expected +QUEUED, got %v", response)
	}

	setCmd2 := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "key2"},
			{Type: resp.BulkString, Str: "value2"},
		},
	}
	serializer.Serialize(setCmd2)
	writer.Flush()

	response, err = parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse SET response: %v", err)
	}
	if response.Type != resp.SimpleString || response.Str != "QUEUED" {
		t.Errorf("Expected +QUEUED, got %v", response)
	}

	exec := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "EXEC"},
		},
	}
	serializer.Serialize(exec)
	writer.Flush()

	response, err = parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse EXEC response: %v", err)
	}
	if response.Type != resp.Array {
		t.Errorf("Expected array for EXEC, got %v", response.Type)
	}
	if len(response.Array) != 2 {
		t.Errorf("Expected 2 results, got %d", len(response.Array))
	}

	val, exists := s.Get("key1")
	if !exists || val != "value1" {
		t.Errorf("Expected key1=value1, got %v (exists: %v)", val, exists)
	}

	val, exists = s.Get("key2")
	if !exists || val != "value2" {
		t.Errorf("Expected key2=value2, got %v (exists: %v)", val, exists)
	}
}

func TestTransactionDiscard(t *testing.T) {
	server := NewServer()
	s := store.NewStore()

	server.RegisterCommand("SET", command.SetCommand(s))

	go server.Start("16380")
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "localhost:16380")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	parser := resp.NewParser(bufio.NewReader(conn))
	serializer := resp.NewSerializer(writer)

	multi := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "MULTI"},
		},
	}
	serializer.Serialize(multi)
	writer.Flush()
	parser.Parse()

	setCmd := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "value1"},
		},
	}
	serializer.Serialize(setCmd)
	writer.Flush()
	parser.Parse()

	discard := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "DISCARD"},
		},
	}
	serializer.Serialize(discard)
	writer.Flush()

	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse DISCARD response: %v", err)
	}
	if response.Type != resp.SimpleString || response.Str != "OK" {
		t.Errorf("Expected +OK for DISCARD, got %v", response)
	}

	_, exists := s.Get("key1")
	if exists {
		t.Error("Expected key1 to not exist after DISCARD")
	}
}

func TestTransactionErrors(t *testing.T) {
	server := NewServer()

	go server.Start("16381")
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "localhost:16381")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	parser := resp.NewParser(bufio.NewReader(conn))
	serializer := resp.NewSerializer(writer)

	exec := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "EXEC"},
		},
	}
	serializer.Serialize(exec)
	writer.Flush()

	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse EXEC response: %v", err)
	}
	if response.Type != resp.Error {
		t.Errorf("Expected error for EXEC without MULTI, got %v", response.Type)
	}

	discard := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "DISCARD"},
		},
	}
	serializer.Serialize(discard)
	writer.Flush()

	response, err = parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse DISCARD response: %v", err)
	}
	if response.Type != resp.Error {
		t.Errorf("Expected error for DISCARD without MULTI, got %v", response.Type)
	}

	multi := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "MULTI"},
		},
	}
	serializer.Serialize(multi)
	writer.Flush()
	parser.Parse()

	serializer.Serialize(multi)
	writer.Flush()

	response, err = parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse nested MULTI response: %v", err)
	}
	if response.Type != resp.Error {
		t.Errorf("Expected error for nested MULTI, got %v", response.Type)
	}
}
