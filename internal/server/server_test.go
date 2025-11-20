package server

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/lojhan/redis-clone/internal/resp"
)

func TestServerStartStop(t *testing.T) {
	server := NewServer()

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start("16379")
	}()

	time.Sleep(100 * time.Millisecond)

	server.mu.RLock()
	running := server.running
	server.mu.RUnlock()

	if !running {
		t.Fatal("Server should be running")
	}

	if err := server.Stop(); err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	select {
	case err := <-errChan:
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Fatalf("Unexpected error from Start: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Server did not stop in time")
	}
}

func TestClientConnection(t *testing.T) {
	server := NewServer()

	server.RegisterCommand("PING", func(args []resp.Value) resp.Value {
		return resp.PongValue()
	})

	go server.Start("16380")
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := net.Dial("tcp", "localhost:16380")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	if server.ClientCount() != 1 {
		t.Errorf("Expected 1 client, got %d", server.ClientCount())
	}

	writer := bufio.NewWriter(conn)
	pingCmd := "*1\r\n$4\r\nPING\r\n"
	writer.WriteString(pingCmd)
	writer.Flush()

	reader := bufio.NewReader(conn)
	parser := resp.NewParser(reader)
	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response.Type != resp.SimpleString || response.Str != "PONG" {
		t.Errorf("Expected PONG, got %+v", response)
	}
}

func TestMultipleClients(t *testing.T) {
	server := NewServer()

	server.RegisterCommand("PING", func(args []resp.Value) resp.Value {
		if len(args) == 0 {
			return resp.PongValue()
		}
		return args[0]
	})

	go server.Start("16381")
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	numClients := 5
	clients := make([]net.Conn, numClients)
	for i := range numClients {
		conn, err := net.Dial("tcp", "localhost:16381")
		if err != nil {
			t.Fatalf("Failed to connect client %d: %v", i, err)
		}
		clients[i] = conn
		defer conn.Close()
	}

	time.Sleep(100 * time.Millisecond)

	if server.ClientCount() != numClients {
		t.Errorf("Expected %d clients, got %d", numClients, server.ClientCount())
	}

	for i, conn := range clients {
		writer := bufio.NewWriter(conn)
		msg := "hello" + string(rune('0'+i))

		cmd := "*2\r\n$4\r\nPING\r\n$" + string(rune('0'+len(msg))) + "\r\n" + msg + "\r\n"
		writer.WriteString(cmd)
		writer.Flush()

		reader := bufio.NewReader(conn)
		parser := resp.NewParser(reader)
		response, err := parser.Parse()
		if err != nil {
			t.Fatalf("Client %d: Failed to parse response: %v", i, err)
		}

		if response.Type != resp.BulkString || response.Str != msg {
			t.Errorf("Client %d: Expected %s, got %+v", i, msg, response)
		}
	}
}

func TestUnknownCommand(t *testing.T) {
	server := NewServer()

	go server.Start("16382")
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := net.Dial("tcp", "localhost:16382")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	cmd := "*1\r\n$7\r\nUNKNOWN\r\n"
	writer.WriteString(cmd)
	writer.Flush()

	reader := bufio.NewReader(conn)
	parser := resp.NewParser(reader)
	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response.Type != resp.Error {
		t.Errorf("Expected Error type, got %v", response.Type)
	}
	if !strings.Contains(response.Str, "unknown command") {
		t.Errorf("Expected 'unknown command' error, got: %s", response.Str)
	}
}

func TestInvalidProtocol(t *testing.T) {
	server := NewServer()

	go server.Start("16383")
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := net.Dial("tcp", "localhost:16383")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	writer.WriteString("+INVALID\r\n")
	writer.Flush()

	reader := bufio.NewReader(conn)
	parser := resp.NewParser(reader)
	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response.Type != resp.Error {
		t.Errorf("Expected Error type, got %v", response.Type)
	}
	if !strings.Contains(response.Str, "protocol error") {
		t.Errorf("Expected 'protocol error', got: %s", response.Str)
	}
}

func TestCommandRegistration(t *testing.T) {
	server := NewServer()

	server.RegisterCommand("CUSTOM", func(args []resp.Value) resp.Value {
		return resp.SimpleStringValue("CUSTOM_RESPONSE")
	})

	server.mu.RLock()
	_, exists := server.handlers["CUSTOM"]
	server.mu.RUnlock()

	if !exists {
		t.Error("Custom command handler not registered")
	}

	go server.Start("16384")
	time.Sleep(100 * time.Millisecond)
	defer server.Stop()

	conn, err := net.Dial("tcp", "localhost:16384")
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	writer := bufio.NewWriter(conn)
	cmd := "*1\r\n$6\r\nCUSTOM\r\n"
	writer.WriteString(cmd)
	writer.Flush()

	reader := bufio.NewReader(conn)
	parser := resp.NewParser(reader)
	response, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if response.Type != resp.SimpleString || response.Str != "CUSTOM_RESPONSE" {
		t.Errorf("Expected CUSTOM_RESPONSE, got %+v", response)
	}
}
