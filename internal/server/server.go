package server

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"syscall"

	"github.com/lojhan/redis-clone/internal/persistence"
	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/panjf2000/gnet/v2"
)

const (
	DefaultPort = "6379"
	MaxClients  = 10000
)

type CommandHandler func(args []resp.Value) resp.Value

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

type Client struct {
	readBuffer    []byte
	inTransaction bool
	txQueue       []resp.Value
	watchedKeys   map[string]bool
	isDirty       bool
}

type Server struct {
	gnet.BuiltinEventEngine

	eng         gnet.Engine
	addr        string
	handlers    map[string]CommandHandler
	clients     map[gnet.Conn]*Client
	watchedKeys map[string][]*Client
	aofWriter   *persistence.AOFWriter
	aofEnabled  bool
}

func NewServer() *Server {
	return &Server{
		handlers:    make(map[string]CommandHandler),
		clients:     make(map[gnet.Conn]*Client),
		watchedKeys: make(map[string][]*Client),
		aofEnabled:  false,
	}
}

func (s *Server) RegisterCommand(name string, handler CommandHandler) {
	s.handlers[strings.ToUpper(name)] = handler
}

func (s *Server) GetHandler(name string) CommandHandler {
	return s.handlers[strings.ToUpper(name)]
}

func (s *Server) SetAOFWriter(aof *persistence.AOFWriter) {
	s.aofWriter = aof
	s.aofEnabled = true
}

func (s *Server) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	log.Printf("Redis event loop server listening on %s", s.addr)
	return gnet.None
}

func (s *Server) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	s.clients[c] = &Client{
		readBuffer:    make([]byte, 0, 4096),
		watchedKeys:   make(map[string]bool),
		inTransaction: false,
	}
	log.Printf("Client connected: %s", c.RemoteAddr())
	return nil, gnet.None
}

func (s *Server) OnTraffic(c gnet.Conn) gnet.Action {
	client, exists := s.clients[c]
	if !exists {
		return gnet.Close
	}

	buf, err := c.Next(-1)
	if err != nil {
		log.Printf("Error reading from %s: %v", c.RemoteAddr(), err)
		return gnet.Close
	}

	client.readBuffer = append(client.readBuffer, buf...)

	for len(client.readBuffer) > 0 {
		parser := resp.NewParser(bytes.NewReader(client.readBuffer))
		value, err := parser.Parse()

		if err != nil {

			if err.Error() == "EOF" || strings.Contains(err.Error(), "EOF") {

				break
			}

			log.Printf("Error parsing command from %s: %v", c.RemoteAddr(), err)
			response := resp.ErrorValue("ERR protocol error")
			s.writeResponse(c, response)
			return gnet.Close
		}

		consumed := s.calculateConsumedBytes(client.readBuffer, value)
		if consumed == 0 {

			break
		}

		response := s.processCommand(client, value)

		s.writeResponse(c, response)

		client.readBuffer = client.readBuffer[consumed:]
	}

	return gnet.None
}

func (s *Server) OnClose(c gnet.Conn, err error) gnet.Action {
	client, exists := s.clients[c]
	if exists {
		s.unwatchAll(client)
		delete(s.clients, c)
	}

	if err != nil {
		log.Printf("Client disconnected: %s (error: %v)", c.RemoteAddr(), err)
	} else {
		log.Printf("Client disconnected: %s", c.RemoteAddr())
	}

	return gnet.None
}

func (s *Server) processCommand(client *Client, value resp.Value) resp.Value {
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
		if client.inTransaction {
			return resp.ErrorValue("ERR MULTI calls can not be nested")
		}
		client.inTransaction = true
		client.txQueue = make([]resp.Value, 0)
		return resp.Value{Type: resp.SimpleString, Str: "OK"}

	case "EXEC":
		if !client.inTransaction {
			return resp.ErrorValue("ERR EXEC without MULTI")
		}

		if client.isDirty {
			client.inTransaction = false
			client.txQueue = nil
			client.isDirty = false
			s.unwatchAll(client)
			return resp.Value{Type: resp.BulkString, Null: true}
		}

		results := make([]resp.Value, len(client.txQueue))
		for i, cmd := range client.txQueue {
			results[i] = s.executeCommand(cmd)
		}

		client.inTransaction = false
		client.txQueue = nil
		s.unwatchAll(client)
		return resp.Value{Type: resp.Array, Array: results}

	case "DISCARD":
		if !client.inTransaction {
			return resp.ErrorValue("ERR DISCARD without MULTI")
		}

		client.inTransaction = false
		client.txQueue = nil
		s.unwatchAll(client)
		return resp.Value{Type: resp.SimpleString, Str: "OK"}

	case "WATCH":
		if client.inTransaction {
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
			s.watchKey(client, keyVal.Str)
		}
		return resp.Value{Type: resp.SimpleString, Str: "OK"}

	case "UNWATCH":
		s.unwatchAll(client)
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	}

	if client.inTransaction {
		client.txQueue = append(client.txQueue, value)
		return resp.Value{Type: resp.SimpleString, Str: "QUEUED"}
	}

	return s.executeCommand(value)
}

func (s *Server) executeCommand(value resp.Value) resp.Value {
	cmdName := strings.ToUpper(value.Array[0].Str)

	handler, exists := s.handlers[cmdName]
	if !exists {
		return resp.ErrorValue(fmt.Sprintf("ERR unknown command '%s'", cmdName))
	}

	args := value.Array[1:]
	result := handler(args)

	if s.aofEnabled && result.Type != resp.Error && isWriteCommand(cmdName) {
		if err := s.aofWriter.Append(value.Array); err != nil {
			log.Printf("Failed to append to AOF: %v", err)
		}
	}

	return result
}

func (s *Server) writeResponse(c gnet.Conn, value resp.Value) {
	var buf bytes.Buffer
	serializer := resp.NewSerializer(&buf)
	if err := serializer.Serialize(value); err != nil {
		log.Printf("Error serializing response: %v", err)
		return
	}

	if err := c.AsyncWrite(buf.Bytes(), nil); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func (s *Server) watchKey(client *Client, key string) {
	client.watchedKeys[key] = true
	s.watchedKeys[key] = append(s.watchedKeys[key], client)
}

func (s *Server) unwatchAll(client *Client) {
	for key := range client.watchedKeys {
		clients := s.watchedKeys[key]

		for i, c := range clients {
			if c == client {
				s.watchedKeys[key] = append(clients[:i], clients[i+1:]...)
				break
			}
		}

		if len(s.watchedKeys[key]) == 0 {
			delete(s.watchedKeys, key)
		}
	}

	client.watchedKeys = make(map[string]bool)
	client.isDirty = false
}

func (s *Server) MarkKeyModified(key string) {
	clients := s.watchedKeys[key]
	for _, client := range clients {
		client.isDirty = true
	}
}

func (s *Server) calculateConsumedBytes(buf []byte, value resp.Value) int {

	var tempBuf bytes.Buffer
	serializer := resp.NewSerializer(&tempBuf)
	if err := serializer.Serialize(value); err != nil {
		return 0
	}

	serializedLen := tempBuf.Len()
	if serializedLen <= len(buf) {
		return serializedLen
	}

	return 0
}

func (s *Server) Start(port string) error {
	if port == "" {
		port = DefaultPort
	}

	s.addr = "tcp://:" + port

	return gnet.Run(s, s.addr,
		gnet.WithMulticore(false),
		gnet.WithReusePort(true),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
	)
}

func (s *Server) Stop() error {
	ctx := context.Background()
	return s.eng.Stop(ctx)
}
