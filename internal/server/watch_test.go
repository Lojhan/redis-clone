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

func TestWatchSuccess(t *testing.T) {
	server := NewServer()
	s := store.NewStore()
	s.SetKeyModifiedHandler(server.MarkKeyModified)

	server.RegisterCommand("SET", command.SetCommand(s))
	server.RegisterCommand("GET", command.GetCommand(s))

	go server.Start("16382")
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", "localhost:16382")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	parser := resp.NewParser(bufio.NewReader(conn))
	serializer := resp.NewSerializer(writer)

	sendCommand(t, serializer, writer, []string{"SET", "key1", "value1"})
	parser.Parse()

	sendCommand(t, serializer, writer, []string{"WATCH", "key1"})
	response, _ := parser.Parse()
	if response.Type != resp.SimpleString || response.Str != "OK" {
		t.Errorf("Expected OK for WATCH, got %v", response)
	}

	sendCommand(t, serializer, writer, []string{"MULTI"})
	response, _ = parser.Parse()
	if response.Type != resp.SimpleString || response.Str != "OK" {
		t.Errorf("Expected OK for MULTI, got %v", response)
	}

	sendCommand(t, serializer, writer, []string{"SET", "key1", "newvalue"})
	response, _ = parser.Parse()
	if response.Type != resp.SimpleString || response.Str != "QUEUED" {
		t.Errorf("Expected QUEUED, got %v", response)
	}

	sendCommand(t, serializer, writer, []string{"EXEC"})
	response, _ = parser.Parse()
	if response.Type != resp.Array {
		t.Errorf("Expected array for EXEC, got %v", response.Type)
	}
	if len(response.Array) != 1 {
		t.Errorf("Expected 1 result, got %d", len(response.Array))
	}

	val, exists := s.Get("key1")
	if !exists || val != "newvalue" {
		t.Errorf("Expected key1=newvalue, got %v (exists: %v)", val, exists)
	}
}

func TestWatchAbort(t *testing.T) {
	server := NewServer()
	s := store.NewStore()
	s.SetKeyModifiedHandler(server.MarkKeyModified)

	server.RegisterCommand("SET", command.SetCommand(s))

	go server.Start("16383")
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn1, err := net.Dial("tcp", "localhost:16383")
	if err != nil {
		t.Fatalf("Failed to connect client 1: %v", err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("tcp", "localhost:16383")
	if err != nil {
		t.Fatalf("Failed to connect client 2: %v", err)
	}
	defer conn2.Close()

	writer1 := bufio.NewWriter(conn1)
	parser1 := resp.NewParser(bufio.NewReader(conn1))
	serializer1 := resp.NewSerializer(writer1)

	writer2 := bufio.NewWriter(conn2)
	parser2 := resp.NewParser(bufio.NewReader(conn2))
	serializer2 := resp.NewSerializer(writer2)

	sendCommand(t, serializer1, writer1, []string{"SET", "key1", "value1"})
	parser1.Parse()

	sendCommand(t, serializer1, writer1, []string{"WATCH", "key1"})
	parser1.Parse()

	sendCommand(t, serializer1, writer1, []string{"MULTI"})
	parser1.Parse()

	sendCommand(t, serializer1, writer1, []string{"SET", "key1", "client1value"})
	parser1.Parse()

	sendCommand(t, serializer2, writer2, []string{"SET", "key1", "client2value"})
	parser2.Parse()

	sendCommand(t, serializer1, writer1, []string{"EXEC"})
	response, _ := parser1.Parse()
	if response.Type != resp.BulkString || !response.Null {
		t.Errorf("Expected null bulk string for aborted EXEC, got %v (Null: %v)", response.Type, response.Null)
	}

	val, exists := s.Get("key1")
	if !exists || val != "client2value" {
		t.Errorf("Expected key1=client2value, got %v (exists: %v)", val, exists)
	}
}

func TestUnwatch(t *testing.T) {
	server := NewServer()
	s := store.NewStore()
	s.SetKeyModifiedHandler(server.MarkKeyModified)

	server.RegisterCommand("SET", command.SetCommand(s))

	go server.Start("16384")
	defer server.Stop()

	time.Sleep(100 * time.Millisecond)

	conn1, err := net.Dial("tcp", "localhost:16384")
	if err != nil {
		t.Fatalf("Failed to connect client 1: %v", err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("tcp", "localhost:16384")
	if err != nil {
		t.Fatalf("Failed to connect client 2: %v", err)
	}
	defer conn2.Close()

	writer1 := bufio.NewWriter(conn1)
	parser1 := resp.NewParser(bufio.NewReader(conn1))
	serializer1 := resp.NewSerializer(writer1)

	writer2 := bufio.NewWriter(conn2)
	parser2 := resp.NewParser(bufio.NewReader(conn2))
	serializer2 := resp.NewSerializer(writer2)

	sendCommand(t, serializer1, writer1, []string{"WATCH", "key1"})
	parser1.Parse()

	sendCommand(t, serializer1, writer1, []string{"UNWATCH"})
	response, _ := parser1.Parse()
	if response.Type != resp.SimpleString || response.Str != "OK" {
		t.Errorf("Expected OK for UNWATCH, got %v", response)
	}

	sendCommand(t, serializer1, writer1, []string{"MULTI"})
	parser1.Parse()

	sendCommand(t, serializer1, writer1, []string{"SET", "key1", "client1value"})
	parser1.Parse()

	sendCommand(t, serializer2, writer2, []string{"SET", "key1", "client2value"})
	parser2.Parse()

	sendCommand(t, serializer1, writer1, []string{"EXEC"})
	response, _ = parser1.Parse()
	if response.Type != resp.Array {
		t.Errorf("Expected array for EXEC, got %v", response.Type)
	}

	val, exists := s.Get("key1")
	if !exists || val != "client1value" {
		t.Errorf("Expected key1=client1value, got %v (exists: %v)", val, exists)
	}
}

func sendCommand(t *testing.T, serializer *resp.Serializer, writer *bufio.Writer, args []string) {
	cmd := resp.Value{
		Type:  resp.Array,
		Array: make([]resp.Value, len(args)),
	}
	for i, arg := range args {
		cmd.Array[i] = resp.Value{
			Type: resp.BulkString,
			Str:  arg,
		}
	}
	if err := serializer.Serialize(cmd); err != nil {
		t.Fatalf("Failed to serialize command: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
}
