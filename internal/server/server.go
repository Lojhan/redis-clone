package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"syscall"

	"github.com/lojhan/redis-clone/internal/persistence"
	"github.com/lojhan/redis-clone/internal/resp"
)

const (
	DefaultPort = "6379"
	MaxClients  = 10000
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	parser *resp.Parser
	server *Server
	mu     sync.Mutex

	inTransaction bool
	txQueue       []resp.Value

	watchedKeys map[string]bool
	isDirty     bool
}

type CommandHandler func(args []resp.Value) resp.Value

type Server struct {
	listener     net.Listener
	clients      map[net.Conn]*Client
	mu           sync.RWMutex
	handlers     map[string]CommandHandler
	running      bool
	wg           sync.WaitGroup
	watchedKeys  map[string][]*Client
	watchedKeyMu sync.RWMutex
	aofWriter    *persistence.AOFWriter
	aofEnabled   bool
}

func NewServer() *Server {
	return &Server{
		clients:     make(map[net.Conn]*Client),
		handlers:    make(map[string]CommandHandler),
		watchedKeys: make(map[string][]*Client),
		running:     false,
		aofEnabled:  false,
	}
}

func (s *Server) RegisterCommand(name string, handler CommandHandler) {
	s.handlers[strings.ToUpper(name)] = handler
}

func (s *Server) GetHandler(name string) CommandHandler {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.handlers[strings.ToUpper(name)]
}

func (s *Server) SetAOFWriter(aof *persistence.AOFWriter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aofWriter = aof
	s.aofEnabled = true
}

func (s *Server) Start(port string) error {
	if port == "" {
		port = DefaultPort
	}

	addr := ":" + port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %s: %w", port, err)
	}

	s.listener = listener
	s.running = true

	log.Printf("Redis server listening on port %s", port)

	for s.running {
		conn, err := listener.Accept()
		if err != nil {
			if s.running {
				log.Printf("Error accepting connection: %v", err)
			}
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			if err := tcpConn.SetNoDelay(true); err != nil {
				log.Printf("Error setting TCP_NODELAY: %v", err)
			}
		}

		s.wg.Add(1)
		go s.handleClient(conn)
	}

	return nil
}

func (s *Server) handleClient(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	client := &Client{
		conn:        conn,
		reader:      bufio.NewReader(conn),
		writer:      bufio.NewWriter(conn),
		server:      s,
		watchedKeys: make(map[string]bool),
	}
	client.parser = resp.NewParser(client.reader)

	s.mu.Lock()
	s.clients[conn] = client
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.clients, conn)
		s.mu.Unlock()

		client.unwatchAll()
	}()

	log.Printf("Client connected: %s", conn.RemoteAddr())

	for {
		value, err := client.parser.Parse()
		if err != nil {
			if err == io.EOF {
				log.Printf("Client disconnected: %s", conn.RemoteAddr())
				return
			}
			log.Printf("Error parsing command from %s: %v", conn.RemoteAddr(), err)

			errResp := resp.ErrorValue("ERR protocol error")
			client.writeResponse(errResp)
			return
		}

		response := client.processCommand(value)

		if err := client.writeResponse(response); err != nil {
			log.Printf("Error writing response to %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

func (c *Client) processCommand(value resp.Value) resp.Value {

	if value.Type != resp.Array {
		return resp.ErrorValue("ERR protocol error: expected array")
	}

	if len(value.Array) == 0 {
		return resp.ErrorValue("ERR empty command")
	}

	cmdValue := value.Array[0]
	if cmdValue.Type != resp.BulkString {
		return resp.ErrorValue("ERR protocol error: command must be bulk string")
	}

	cmdName := strings.ToUpper(cmdValue.Str)

	switch cmdName {
	case "MULTI":
		if c.inTransaction {
			return resp.ErrorValue("ERR MULTI calls can not be nested")
		}
		c.inTransaction = true
		c.txQueue = make([]resp.Value, 0)
		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}

	case "EXEC":
		if !c.inTransaction {
			return resp.ErrorValue("ERR EXEC without MULTI")
		}

		if c.isDirty {

			c.inTransaction = false
			c.txQueue = nil
			c.isDirty = false
			c.unwatchAll()

			return resp.Value{
				Type: resp.BulkString,
				Null: true,
			}
		}

		results := make([]resp.Value, len(c.txQueue))
		for i, cmd := range c.txQueue {
			results[i] = c.executeCommand(cmd)
		}

		c.inTransaction = false
		c.txQueue = nil
		c.unwatchAll()
		return resp.Value{
			Type:  resp.Array,
			Array: results,
		}

	case "DISCARD":
		if !c.inTransaction {
			return resp.ErrorValue("ERR DISCARD without MULTI")
		}

		c.inTransaction = false
		c.txQueue = nil
		c.unwatchAll()
		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}

	case "WATCH":

		if c.inTransaction {
			return resp.ErrorValue("ERR WATCH inside MULTI is not allowed")
		}

		keys := value.Array[1:]
		if len(keys) == 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'watch' command")
		}

		for _, keyVal := range keys {
			if keyVal.Type != resp.BulkString {
				return resp.ErrorValue("ERR protocol error")
			}
			c.watchKey(keyVal.Str)
		}
		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}

	case "UNWATCH":
		c.unwatchAll()
		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}

	if c.inTransaction {
		c.txQueue = append(c.txQueue, value)
		return resp.Value{
			Type: resp.SimpleString,
			Str:  "QUEUED",
		}
	}

	return c.executeCommand(value)
}

func (c *Client) executeCommand(value resp.Value) resp.Value {
	cmdName := strings.ToUpper(value.Array[0].Str)

	c.server.mu.RLock()
	handler, exists := c.server.handlers[cmdName]
	aofEnabled := c.server.aofEnabled
	aofWriter := c.server.aofWriter
	c.server.mu.RUnlock()

	if !exists {
		return resp.ErrorValue(fmt.Sprintf("ERR unknown command '%s'", cmdName))
	}

	args := value.Array[1:]

	result := handler(args)

	if aofEnabled && result.Type != resp.Error && isWriteCommand(cmdName) {
		if err := aofWriter.Append(value.Array); err != nil {
			log.Printf("Failed to append to AOF: %v", err)
		}
	}

	return result
}

func (c *Client) writeResponse(value resp.Value) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	serializer := resp.NewSerializer(c.writer)
	if err := serializer.Serialize(value); err != nil {
		return err
	}

	return c.writer.Flush()
}

func (s *Server) Stop() error {
	s.running = false

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return err
		}
	}

	s.mu.Lock()
	for conn := range s.clients {
		conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()

	log.Println("Server stopped")
	return nil
}

func (s *Server) ClientCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.clients)
}

func (c *Client) watchKey(key string) {

	c.watchedKeys[key] = true

	c.server.watchedKeyMu.Lock()
	c.server.watchedKeys[key] = append(c.server.watchedKeys[key], c)
	c.server.watchedKeyMu.Unlock()
}

func (c *Client) unwatchAll() {

	c.server.watchedKeyMu.Lock()
	for key := range c.watchedKeys {
		clients := c.server.watchedKeys[key]

		for i, client := range clients {
			if client == c {

				c.server.watchedKeys[key] = append(clients[:i], clients[i+1:]...)
				break
			}
		}

		if len(c.server.watchedKeys[key]) == 0 {
			delete(c.server.watchedKeys, key)
		}
	}
	c.server.watchedKeyMu.Unlock()

	c.watchedKeys = make(map[string]bool)
	c.isDirty = false
}

func (s *Server) MarkKeyModified(key string) {
	s.watchedKeyMu.RLock()
	clients := s.watchedKeys[key]
	s.watchedKeyMu.RUnlock()

	for _, client := range clients {
		client.mu.Lock()
		client.isDirty = true
		client.mu.Unlock()
	}
}

func isWriteCommand(cmdName string) bool {
	writeCommands := map[string]bool{
		"SET":       true,
		"DEL":       true,
		"INCR":      true,
		"DECR":      true,
		"LPUSH":     true,
		"RPUSH":     true,
		"LPOP":      true,
		"RPOP":      true,
		"HSET":      true,
		"HDEL":      true,
		"SADD":      true,
		"SREM":      true,
		"SPOP":      true,
		"ZADD":      true,
		"ZREM":      true,
		"EXPIRE":    true,
		"PEXPIREAT": true,
		"FLUSHDB":   true,
		"FLUSHALL":  true,
	}
	return writeCommands[cmdName]
}

func SetNonBlocking(fd int) error {
	return syscall.SetNonblock(fd, true)
}
