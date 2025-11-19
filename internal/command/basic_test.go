package command

import (
	"strings"
	"testing"

	"github.com/lojhan/redis-clone/internal/resp"
)

func TestPingCommand(t *testing.T) {
	tests := []struct {
		name     string
		args     []resp.Value
		expected resp.Value
	}{
		{
			name:     "PING without argument",
			args:     []resp.Value{},
			expected: resp.PongValue(),
		},
		{
			name: "PING with message",
			args: []resp.Value{
				resp.BulkStringValue("hello"),
			},
			expected: resp.BulkStringValue("hello"),
		},
		{
			name: "PING with multiple arguments (error)",
			args: []resp.Value{
				resp.BulkStringValue("hello"),
				resp.BulkStringValue("world"),
			},
			expected: resp.ErrorValue("ERR wrong number of arguments for 'ping' command"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PingCommand(tt.args)

			if result.Type != tt.expected.Type {
				t.Errorf("Expected type %v, got %v", tt.expected.Type, result.Type)
			}

			if result.Type == resp.SimpleString || result.Type == resp.Error || result.Type == resp.BulkString {
				if result.Str != tt.expected.Str {
					t.Errorf("Expected %q, got %q", tt.expected.Str, result.Str)
				}
			}
		})
	}
}

func TestEchoCommand(t *testing.T) {
	tests := []struct {
		name     string
		args     []resp.Value
		expected resp.Value
	}{
		{
			name: "ECHO with message",
			args: []resp.Value{
				resp.BulkStringValue("hello world"),
			},
			expected: resp.BulkStringValue("hello world"),
		},
		{
			name:     "ECHO without argument (error)",
			args:     []resp.Value{},
			expected: resp.ErrorValue("ERR wrong number of arguments for 'echo' command"),
		},
		{
			name: "ECHO with multiple arguments (error)",
			args: []resp.Value{
				resp.BulkStringValue("hello"),
				resp.BulkStringValue("world"),
			},
			expected: resp.ErrorValue("ERR wrong number of arguments for 'echo' command"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := EchoCommand(tt.args)

			if result.Type != tt.expected.Type {
				t.Errorf("Expected type %v, got %v", tt.expected.Type, result.Type)
			}

			if result.Type == resp.BulkString || result.Type == resp.Error {
				if result.Str != tt.expected.Str {
					t.Errorf("Expected %q, got %q", tt.expected.Str, result.Str)
				}
			}
		})
	}
}

func TestCommandCommand(t *testing.T) {
	result := CommandCommand([]resp.Value{})

	if result.Type != resp.Array {
		t.Errorf("Expected Array type, got %v", result.Type)
	}
}

func TestInfoCommand(t *testing.T) {
	tests := []struct {
		name     string
		args     []resp.Value
		contains string
	}{
		{
			name:     "INFO without argument",
			args:     []resp.Value{},
			contains: "redis_version",
		},
		{
			name: "INFO server",
			args: []resp.Value{
				resp.BulkStringValue("server"),
			},
			contains: "# Server",
		},
		{
			name: "INFO clients",
			args: []resp.Value{
				resp.BulkStringValue("clients"),
			},
			contains: "# Clients",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InfoCommand(tt.args)

			if result.Type != resp.BulkString {
				t.Errorf("Expected BulkString type, got %v", result.Type)
			}

			if !strings.Contains(result.Str, tt.contains) {
				t.Errorf("Expected result to contain %q, got %q", tt.contains, result.Str)
			}
		})
	}
}

func TestConfigCommand(t *testing.T) {
	tests := []struct {
		name        string
		args        []resp.Value
		expectError bool
	}{
		{
			name:        "CONFIG without arguments (error)",
			args:        []resp.Value{},
			expectError: true,
		},
		{
			name: "CONFIG GET",
			args: []resp.Value{
				resp.BulkStringValue("GET"),
				resp.BulkStringValue("maxmemory"),
			},
			expectError: false,
		},
		{
			name: "CONFIG SET",
			args: []resp.Value{
				resp.BulkStringValue("SET"),
				resp.BulkStringValue("maxmemory"),
				resp.BulkStringValue("100mb"),
			},
			expectError: false,
		},
		{
			name: "CONFIG unknown subcommand",
			args: []resp.Value{
				resp.BulkStringValue("UNKNOWN"),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConfigCommand(tt.args)

			if tt.expectError {
				if result.Type != resp.Error {
					t.Errorf("Expected Error type, got %v", result.Type)
				}
			} else {
				if result.Type == resp.Error {
					t.Errorf("Unexpected error: %s", result.Str)
				}
			}
		})
	}
}
