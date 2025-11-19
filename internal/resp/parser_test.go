package resp

import (
	"strings"
	"testing"
)

func TestParseSimpleString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		wantErr  bool
	}{
		{
			name:     "basic simple string",
			input:    "+OK\r\n",
			expected: Value{Type: SimpleString, Str: "OK"},
			wantErr:  false,
		},
		{
			name:     "simple string with spaces",
			input:    "+hello world\r\n",
			expected: Value{Type: SimpleString, Str: "hello world"},
			wantErr:  false,
		},
		{
			name:     "empty simple string",
			input:    "+\r\n",
			expected: Value{Type: SimpleString, Str: ""},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			got, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if got.Type != tt.expected.Type || got.Str != tt.expected.Str {
					t.Errorf("Parse() = %+v, want %+v", got, tt.expected)
				}
			}
		})
	}
}

func TestParseError(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		wantErr  bool
	}{
		{
			name:     "basic error",
			input:    "-ERR unknown command\r\n",
			expected: Value{Type: Error, Str: "ERR unknown command"},
			wantErr:  false,
		},
		{
			name:     "error with details",
			input:    "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
			expected: Value{Type: Error, Str: "WRONGTYPE Operation against a key holding the wrong kind of value"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			got, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if got.Type != tt.expected.Type || got.Str != tt.expected.Str {
					t.Errorf("Parse() = %+v, want %+v", got, tt.expected)
				}
			}
		})
	}
}

func TestParseInteger(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		wantErr  bool
	}{
		{
			name:     "positive integer",
			input:    ":1000\r\n",
			expected: Value{Type: Integer, Int: 1000},
			wantErr:  false,
		},
		{
			name:     "negative integer",
			input:    ":-42\r\n",
			expected: Value{Type: Integer, Int: -42},
			wantErr:  false,
		},
		{
			name:     "zero",
			input:    ":0\r\n",
			expected: Value{Type: Integer, Int: 0},
			wantErr:  false,
		},
		{
			name:     "invalid integer",
			input:    ":abc\r\n",
			expected: Value{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			got, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if got.Type != tt.expected.Type || got.Int != tt.expected.Int {
					t.Errorf("Parse() = %+v, want %+v", got, tt.expected)
				}
			}
		})
	}
}

func TestParseBulkString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		wantErr  bool
	}{
		{
			name:     "basic bulk string",
			input:    "$5\r\nhello\r\n",
			expected: Value{Type: BulkString, Str: "hello"},
			wantErr:  false,
		},
		{
			name:     "empty bulk string",
			input:    "$0\r\n\r\n",
			expected: Value{Type: BulkString, Str: ""},
			wantErr:  false,
		},
		{
			name:     "null bulk string",
			input:    "$-1\r\n",
			expected: Value{Type: BulkString, Null: true},
			wantErr:  false,
		},
		{
			name:     "bulk string with special characters",
			input:    "$11\r\nhello\nworld\r\n",
			expected: Value{Type: BulkString, Str: "hello\nworld"},
			wantErr:  false,
		},
		{
			name:     "binary-safe bulk string",
			input:    "$6\r\nhel\x00lo\r\n",
			expected: Value{Type: BulkString, Str: "hel\x00lo"},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			got, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if got.Type != tt.expected.Type || got.Str != tt.expected.Str || got.Null != tt.expected.Null {
					t.Errorf("Parse() = %+v, want %+v", got, tt.expected)
				}
			}
		})
	}
}

func TestParseArray(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Value
		wantErr  bool
	}{
		{
			name:  "array of bulk strings",
			input: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
			expected: Value{
				Type: Array,
				Array: []Value{
					{Type: BulkString, Str: "hello"},
					{Type: BulkString, Str: "world"},
				},
			},
			wantErr: false,
		},
		{
			name:  "empty array",
			input: "*0\r\n",
			expected: Value{
				Type:  Array,
				Array: []Value{},
			},
			wantErr: false,
		},
		{
			name:     "null array",
			input:    "*-1\r\n",
			expected: Value{Type: Array, Null: true},
			wantErr:  false,
		},
		{
			name:  "array with mixed types",
			input: "*3\r\n:1\r\n$5\r\nhello\r\n+OK\r\n",
			expected: Value{
				Type: Array,
				Array: []Value{
					{Type: Integer, Int: 1},
					{Type: BulkString, Str: "hello"},
					{Type: SimpleString, Str: "OK"},
				},
			},
			wantErr: false,
		},
		{
			name:  "nested array",
			input: "*2\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n:42\r\n",
			expected: Value{
				Type: Array,
				Array: []Value{
					{
						Type: Array,
						Array: []Value{
							{Type: BulkString, Str: "hello"},
							{Type: BulkString, Str: "world"},
						},
					},
					{Type: Integer, Int: 42},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := NewParser(strings.NewReader(tt.input))
			got, err := parser.Parse()

			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if !valuesEqual(got, tt.expected) {
					t.Errorf("Parse() = %+v, want %+v", got, tt.expected)
				}
			}
		})
	}
}

func TestParseRedisCommand(t *testing.T) {

	input := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	parser := NewParser(strings.NewReader(input))
	got, err := parser.Parse()

	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	expected := Value{
		Type: Array,
		Array: []Value{
			{Type: BulkString, Str: "SET"},
			{Type: BulkString, Str: "key"},
			{Type: BulkString, Str: "value"},
		},
	}

	if !valuesEqual(got, expected) {
		t.Errorf("Parse() = %+v, want %+v", got, expected)
	}
}

func valuesEqual(a, b Value) bool {
	if a.Type != b.Type || a.Null != b.Null {
		return false
	}

	switch a.Type {
	case SimpleString, Error:
		return a.Str == b.Str
	case Integer:
		return a.Int == b.Int
	case BulkString:
		if a.Null || b.Null {
			return a.Null == b.Null
		}
		return a.Str == b.Str
	case Array:
		if a.Null || b.Null {
			return a.Null == b.Null
		}
		if len(a.Array) != len(b.Array) {
			return false
		}
		for i := range a.Array {
			if !valuesEqual(a.Array[i], b.Array[i]) {
				return false
			}
		}
		return true
	}
	return false
}
