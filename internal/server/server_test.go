package server

import (
	"bytes"
	"net"
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/panjf2000/gnet/v2"
)

func TestNewServer(t *testing.T) {
	server := NewServer()

	if server == nil {
		t.Fatal("NewServer returned nil")
	}

	if server.handlers == nil {
		t.Error("handlers map not initialized")
	}

	if server.clients == nil {
		t.Error("clients map not initialized")
	}

	if server.watchedKeys == nil {
		t.Error("watchedKeys map not initialized")
	}

	if server.aofEnabled {
		t.Error("AOF should not be enabled by default")
	}
}

func TestRegisterCommand(t *testing.T) {
	server := NewServer()

	testHandler := func(args []resp.Value) resp.Value {
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	}

	server.RegisterCommand("TEST", testHandler)

	handler := server.GetHandler("TEST")
	if handler == nil {
		t.Fatal("Handler not registered")
	}

	handler = server.GetHandler("test")
	if handler == nil {
		t.Error("Handler should be case-insensitive")
	}

	handler = server.GetHandler("TeSt")
	if handler == nil {
		t.Error("Handler should be case-insensitive")
	}
}

func TestGetHandlerNonExistent(t *testing.T) {
	server := NewServer()

	handler := server.GetHandler("NONEXISTENT")
	if handler != nil {
		t.Error("Should return nil for non-existent handler")
	}
}

func TestIsWriteCommand(t *testing.T) {
	tests := []struct {
		name     string
		command  string
		expected bool
	}{
		{"SET is write", "SET", true},
		{"GET is not write", "GET", false},
		{"DEL is write", "DEL", true},
		{"INCR is write", "INCR", true},
		{"DECR is write", "DECR", true},
		{"LPUSH is write", "LPUSH", true},
		{"RPUSH is write", "RPUSH", true},
		{"LPOP is write", "LPOP", true},
		{"RPOP is write", "RPOP", true},
		{"HSET is write", "HSET", true},
		{"HDEL is write", "HDEL", true},
		{"SADD is write", "SADD", true},
		{"SREM is write", "SREM", true},
		{"SPOP is write", "SPOP", true},
		{"ZADD is write", "ZADD", true},
		{"ZREM is write", "ZREM", true},
		{"EXPIRE is write", "EXPIRE", true},
		{"PEXPIREAT is write", "PEXPIREAT", true},
		{"FLUSHDB is write", "FLUSHDB", true},
		{"FLUSHALL is write", "FLUSHALL", true},
		{"KEYS is not write", "KEYS", false},
		{"SCAN is not write", "SCAN", false},
		{"UNKNOWN is not write", "UNKNOWN", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isWriteCommand(tt.command)
			if result != tt.expected {
				t.Errorf("isWriteCommand(%s) = %v, want %v", tt.command, result, tt.expected)
			}
		})
	}
}

func TestProcessCommandInvalidType(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{Type: resp.SimpleString, Str: "INVALID"}
	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for non-array command, got %v", result.Type)
	}

	if result.Str != "ERR protocol error: expected array" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandEmptyArray(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{Type: resp.Array, Array: []resp.Value{}}
	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for empty command, got %v", result.Type)
	}

	if result.Str != "ERR empty command" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandInvalidCommandType(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.SimpleString, Str: "GET"},
		},
	}
	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for invalid command type, got %v", result.Type)
	}

	if result.Str != "ERR protocol error: command must be bulk string" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandMULTI(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "MULTI"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected +OK for MULTI, got %v", result)
	}

	if !client.inTransaction {
		t.Error("Client should be in transaction after MULTI")
	}

	if client.txQueue == nil {
		t.Error("Transaction queue should be initialized")
	}
}

func TestProcessCommandNestedMULTI(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: true,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "MULTI"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for nested MULTI, got %v", result.Type)
	}

	if result.Str != "ERR MULTI calls can not be nested" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandEXECWithoutMULTI(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "EXEC"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for EXEC without MULTI, got %v", result.Type)
	}

	if result.Str != "ERR EXEC without MULTI" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandEXECDirty(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: true,
		isDirty:       true,
		txQueue:       []resp.Value{},
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "EXEC"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.BulkString || !result.Null {
		t.Errorf("Expected null bulk string for dirty EXEC, got %v", result)
	}

	if client.inTransaction {
		t.Error("Client should not be in transaction after EXEC")
	}

	if client.isDirty {
		t.Error("isDirty should be reset after EXEC")
	}
}

func TestProcessCommandEXECSuccess(t *testing.T) {
	server := NewServer()

	server.RegisterCommand("PING", func(args []resp.Value) resp.Value {
		return resp.Value{Type: resp.SimpleString, Str: "PONG"}
	})

	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: true,
		isDirty:       false,
		txQueue: []resp.Value{
			{
				Type: resp.Array,
				Array: []resp.Value{
					{Type: resp.BulkString, Str: "PING"},
				},
			},
		},
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "EXEC"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.Array {
		t.Errorf("Expected array response for EXEC, got %v", result.Type)
	}

	if len(result.Array) != 1 {
		t.Errorf("Expected 1 result, got %d", len(result.Array))
	}

	if result.Array[0].Type != resp.SimpleString || result.Array[0].Str != "PONG" {
		t.Errorf("Expected PONG response, got %v", result.Array[0])
	}

	if client.inTransaction {
		t.Error("Client should not be in transaction after EXEC")
	}
}

func TestProcessCommandDISCARDWithoutMULTI(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "DISCARD"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for DISCARD without MULTI, got %v", result.Type)
	}

	if result.Str != "ERR DISCARD without MULTI" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandDISCARDSuccess(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: true,
		txQueue:       []resp.Value{},
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "DISCARD"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected +OK for DISCARD, got %v", result)
	}

	if client.inTransaction {
		t.Error("Client should not be in transaction after DISCARD")
	}

	if client.txQueue != nil {
		t.Error("Transaction queue should be nil after DISCARD")
	}
}

func TestProcessCommandWATCH(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "WATCH"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "key2"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected +OK for WATCH, got %v", result)
	}

	if !client.watchedKeys["key1"] || !client.watchedKeys["key2"] {
		t.Error("Keys should be watched")
	}

	if len(server.watchedKeys["key1"]) != 1 || len(server.watchedKeys["key2"]) != 1 {
		t.Error("Keys should be registered in server watchedKeys")
	}
}

func TestProcessCommandWATCHInsideMULTI(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: true,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "WATCH"},
			{Type: resp.BulkString, Str: "key1"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for WATCH inside MULTI, got %v", result.Type)
	}

	if result.Str != "ERR WATCH inside MULTI is not allowed" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandWATCHNoKeys(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "WATCH"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for WATCH without keys, got %v", result.Type)
	}

	if result.Str != "ERR wrong number of arguments for 'watch' command" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestProcessCommandUNWATCH(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   map[string]bool{"key1": true},
		inTransaction: false,
	}
	server.watchedKeys["key1"] = []*Client{client}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "UNWATCH"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.SimpleString || result.Str != "OK" {
		t.Errorf("Expected +OK for UNWATCH, got %v", result)
	}

	if len(client.watchedKeys) != 0 {
		t.Error("Client should not have any watched keys after UNWATCH")
	}

	if len(server.watchedKeys) != 0 {
		t.Error("Server should not have any watched keys after UNWATCH")
	}
}

func TestProcessCommandQueueing(t *testing.T) {
	server := NewServer()
	server.RegisterCommand("PING", func(args []resp.Value) resp.Value {
		return resp.Value{Type: resp.SimpleString, Str: "PONG"}
	})

	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: true,
		txQueue:       []resp.Value{},
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "PING"},
		},
	}

	result := server.processCommand(client, value)

	if result.Type != resp.SimpleString || result.Str != "QUEUED" {
		t.Errorf("Expected QUEUED response, got %v", result)
	}

	if len(client.txQueue) != 1 {
		t.Errorf("Expected 1 command in queue, got %d", len(client.txQueue))
	}
}

func TestExecuteCommandUnknown(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "UNKNOWN"},
		},
	}

	result := server.executeCommand(client, value)

	if result.Type != resp.Error {
		t.Errorf("Expected error for unknown command, got %v", result.Type)
	}

	if result.Str != "ERR unknown command 'UNKNOWN'" {
		t.Errorf("Wrong error message: %s", result.Str)
	}
}

func TestExecuteCommandSuccess(t *testing.T) {
	server := NewServer()
	server.RegisterCommand("ECHO", func(args []resp.Value) resp.Value {
		if len(args) == 0 {
			return resp.ErrorValue("ERR wrong number of arguments")
		}
		return resp.Value{Type: resp.BulkString, Str: args[0].Str}
	})

	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "ECHO"},
			{Type: resp.BulkString, Str: "hello"},
		},
	}

	result := server.executeCommand(client, value)

	if result.Type != resp.BulkString {
		t.Errorf("Expected bulk string response, got %v", result.Type)
	}

	if result.Str != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result.Str)
	}
}

func TestMarkKeyModified(t *testing.T) {
	server := NewServer()

	client1 := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   map[string]bool{"key1": true},
		inTransaction: false,
		isDirty:       false,
	}

	client2 := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   map[string]bool{"key1": true},
		inTransaction: false,
		isDirty:       false,
	}

	server.watchedKeys["key1"] = []*Client{client1, client2}

	server.MarkKeyModified("key1")

	if !client1.isDirty {
		t.Error("client1 should be marked dirty")
	}

	if !client2.isDirty {
		t.Error("client2 should be marked dirty")
	}
}

func TestMarkKeyModifiedNonWatched(t *testing.T) {
	server := NewServer()

	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
		isDirty:       false,
	}

	server.clients[nil] = client

	server.MarkKeyModified("nonexistent")

	if client.isDirty {
		t.Error("client should not be marked dirty for non-watched key")
	}
}

func TestUnwatchAll(t *testing.T) {
	server := NewServer()

	client := &Client{
		readBuffer:    make([]byte, 0),
		watchedKeys:   map[string]bool{"key1": true, "key2": true},
		inTransaction: false,
		isDirty:       true,
	}

	server.watchedKeys["key1"] = []*Client{client}
	server.watchedKeys["key2"] = []*Client{client}

	server.unwatchAll(client)

	if len(client.watchedKeys) != 0 {
		t.Error("Client should have no watched keys")
	}

	if len(server.watchedKeys) != 0 {
		t.Error("Server should have no watched keys")
	}

	if client.isDirty {
		t.Error("isDirty should be reset")
	}
}

func TestUnwatchAllMultipleClients(t *testing.T) {
	server := NewServer()

	client1 := &Client{
		readBuffer:  make([]byte, 0),
		watchedKeys: map[string]bool{"key1": true},
	}

	client2 := &Client{
		readBuffer:  make([]byte, 0),
		watchedKeys: map[string]bool{"key1": true},
	}

	server.watchedKeys["key1"] = []*Client{client1, client2}

	server.unwatchAll(client1)

	if len(client1.watchedKeys) != 0 {
		t.Error("client1 should have no watched keys")
	}

	if len(server.watchedKeys["key1"]) != 1 {
		t.Error("key1 should still have client2 watching")
	}

	if server.watchedKeys["key1"][0] != client2 {
		t.Error("client2 should still be watching key1")
	}
}

func TestCalculateConsumedBytes(t *testing.T) {
	server := NewServer()

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "PING"},
		},
	}

	var buf bytes.Buffer
	serializer := resp.NewSerializer(&buf)
	serializer.Serialize(value)
	expectedBytes := buf.Bytes()

	consumed := server.calculateConsumedBytes(expectedBytes, value)

	if consumed != len(expectedBytes) {
		t.Errorf("Expected %d consumed bytes, got %d", len(expectedBytes), consumed)
	}
}

func TestCalculateConsumedBytesInsufficientBuffer(t *testing.T) {
	server := NewServer()

	value := resp.Value{
		Type: resp.Array,
		Array: []resp.Value{
			{Type: resp.BulkString, Str: "PING"},
		},
	}

	smallBuf := []byte("*1\r\n")

	consumed := server.calculateConsumedBytes(smallBuf, value)

	if consumed != 0 {
		t.Errorf("Expected 0 consumed bytes for insufficient buffer, got %d", consumed)
	}
}

func TestWatchKey(t *testing.T) {
	server := NewServer()
	client := &Client{
		readBuffer:  make([]byte, 0),
		watchedKeys: make(map[string]bool),
	}

	server.watchKey(client, "testkey")

	if !client.watchedKeys["testkey"] {
		t.Error("Key should be in client's watched keys")
	}

	if len(server.watchedKeys["testkey"]) != 1 {
		t.Error("Server should track one client for the key")
	}

	if server.watchedKeys["testkey"][0] != client {
		t.Error("Server should track the correct client")
	}
}

func TestWatchKeyMultipleClients(t *testing.T) {
	server := NewServer()

	client1 := &Client{
		readBuffer:  make([]byte, 0),
		watchedKeys: make(map[string]bool),
	}

	client2 := &Client{
		readBuffer:  make([]byte, 0),
		watchedKeys: make(map[string]bool),
	}

	server.watchKey(client1, "testkey")
	server.watchKey(client2, "testkey")

	if len(server.watchedKeys["testkey"]) != 2 {
		t.Errorf("Expected 2 clients watching the key, got %d", len(server.watchedKeys["testkey"]))
	}
}

func TestClientInitialization(t *testing.T) {
	client := &Client{
		readBuffer:    make([]byte, 0, 4096),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}

	if client.readBuffer == nil {
		t.Error("readBuffer should be initialized")
	}

	if client.watchedKeys == nil {
		t.Error("watchedKeys should be initialized")
	}

	if client.inTransaction {
		t.Error("inTransaction should be false by default")
	}

	if client.isDirty {
		t.Error("isDirty should be false by default")
	}

	if client.txQueue != nil {
		t.Error("txQueue should be nil initially")
	}
}

type mockConn struct {
	gnet.Conn
	remoteAddr string
	writeBuf   []byte
	writeErr   error
}

func (m *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
}

func (m *mockConn) AsyncWrite(data []byte, callback gnet.AsyncCallback) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.writeBuf = append(m.writeBuf, data...)
	return nil
}

func TestOnOpen(t *testing.T) {
	server := NewServer()
	conn := &mockConn{}

	_, action := server.OnOpen(conn)

	if action != gnet.None {
		t.Errorf("Expected gnet.None action, got %v", action)
	}

	client, exists := server.clients[conn]
	if !exists {
		t.Fatal("Client should be registered on connection open")
	}

	if client.readBuffer == nil {
		t.Error("Client readBuffer should be initialized")
	}

	if client.watchedKeys == nil {
		t.Error("Client watchedKeys should be initialized")
	}

	if client.inTransaction {
		t.Error("Client should not be in transaction initially")
	}
}

func TestOnClose(t *testing.T) {
	server := NewServer()
	conn := &mockConn{}

	client := &Client{
		readBuffer:  make([]byte, 0),
		watchedKeys: map[string]bool{"key1": true},
	}
	server.clients[conn] = client
	server.watchedKeys["key1"] = []*Client{client}

	action := server.OnClose(conn, nil)

	if action != gnet.None {
		t.Errorf("Expected gnet.None action, got %v", action)
	}

	if _, exists := server.clients[conn]; exists {
		t.Error("Client should be removed on connection close")
	}

	if len(server.watchedKeys) != 0 {
		t.Error("Watched keys should be cleaned up on connection close")
	}
}

func TestWriteResponse(t *testing.T) {
	server := NewServer()
	conn := &mockConn{}

	response := resp.Value{Type: resp.SimpleString, Str: "OK"}

	server.writeResponse(conn, response)

	if len(conn.writeBuf) == 0 {
		t.Error("Response should be written to connection")
	}

	expected := "+OK\r\n"
	if string(conn.writeBuf) != expected {
		t.Errorf("Expected %q, got %q", expected, string(conn.writeBuf))
	}
}
