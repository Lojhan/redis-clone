package resp

import (
	"bytes"
	"testing"
)

func TestSerializeSimpleString(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name:     "basic simple string",
			value:    SimpleStringValue("OK"),
			expected: "+OK\r\n",
		},
		{
			name:     "simple string with spaces",
			value:    SimpleStringValue("hello world"),
			expected: "+hello world\r\n",
		},
		{
			name:     "empty simple string",
			value:    SimpleStringValue(""),
			expected: "+\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			s := NewSerializer(buf)
			err := s.Serialize(tt.value)

			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if buf.String() != tt.expected {
				t.Errorf("Serialize() = %q, want %q", buf.String(), tt.expected)
			}
		})
	}
}

func TestSerializeError(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name:     "basic error",
			value:    ErrorValue("ERR unknown command"),
			expected: "-ERR unknown command\r\n",
		},
		{
			name:     "error with details",
			value:    ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value"),
			expected: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			s := NewSerializer(buf)
			err := s.Serialize(tt.value)

			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if buf.String() != tt.expected {
				t.Errorf("Serialize() = %q, want %q", buf.String(), tt.expected)
			}
		})
	}
}

func TestSerializeInteger(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name:     "positive integer",
			value:    IntegerValue(1000),
			expected: ":1000\r\n",
		},
		{
			name:     "negative integer",
			value:    IntegerValue(-42),
			expected: ":-42\r\n",
		},
		{
			name:     "zero",
			value:    IntegerValue(0),
			expected: ":0\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			s := NewSerializer(buf)
			err := s.Serialize(tt.value)

			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if buf.String() != tt.expected {
				t.Errorf("Serialize() = %q, want %q", buf.String(), tt.expected)
			}
		})
	}
}

func TestSerializeBulkString(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name:     "basic bulk string",
			value:    BulkStringValue("hello"),
			expected: "$5\r\nhello\r\n",
		},
		{
			name:     "empty bulk string",
			value:    BulkStringValue(""),
			expected: "$0\r\n\r\n",
		},
		{
			name:     "null bulk string",
			value:    NullBulkStringValue(),
			expected: "$-1\r\n",
		},
		{
			name:     "bulk string with newline",
			value:    BulkStringValue("hello\nworld"),
			expected: "$11\r\nhello\nworld\r\n",
		},
		{
			name:     "binary-safe bulk string",
			value:    BulkStringValue("hel\x00lo"),
			expected: "$6\r\nhel\x00lo\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			s := NewSerializer(buf)
			err := s.Serialize(tt.value)

			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if buf.String() != tt.expected {
				t.Errorf("Serialize() = %q, want %q", buf.String(), tt.expected)
			}
		})
	}
}

func TestSerializeArray(t *testing.T) {
	tests := []struct {
		name     string
		value    Value
		expected string
	}{
		{
			name: "array of bulk strings",
			value: ArrayValue(
				BulkStringValue("hello"),
				BulkStringValue("world"),
			),
			expected: "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n",
		},
		{
			name:     "empty array",
			value:    ArrayValue(),
			expected: "*0\r\n",
		},
		{
			name:     "null array",
			value:    NullArrayValue(),
			expected: "*-1\r\n",
		},
		{
			name: "array with mixed types",
			value: ArrayValue(
				IntegerValue(1),
				BulkStringValue("hello"),
				SimpleStringValue("OK"),
			),
			expected: "*3\r\n:1\r\n$5\r\nhello\r\n+OK\r\n",
		},
		{
			name: "nested array",
			value: ArrayValue(
				ArrayValue(
					BulkStringValue("hello"),
					BulkStringValue("world"),
				),
				IntegerValue(42),
			),
			expected: "*2\r\n*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n:42\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			s := NewSerializer(buf)
			err := s.Serialize(tt.value)

			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			if buf.String() != tt.expected {
				t.Errorf("Serialize() = %q, want %q", buf.String(), tt.expected)
			}
		})
	}
}

func TestSerializeRedisCommand(t *testing.T) {

	value := ArrayValue(
		BulkStringValue("SET"),
		BulkStringValue("key"),
		BulkStringValue("value"),
	)

	buf := new(bytes.Buffer)
	s := NewSerializer(buf)
	err := s.Serialize(value)

	if err != nil {
		t.Fatalf("Serialize() error = %v", err)
	}

	expected := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	if buf.String() != expected {
		t.Errorf("Serialize() = %q, want %q", buf.String(), expected)
	}
}

func TestRoundTrip(t *testing.T) {

	tests := []Value{
		SimpleStringValue("OK"),
		ErrorValue("ERR test"),
		IntegerValue(42),
		BulkStringValue("hello"),
		NullBulkStringValue(),
		ArrayValue(
			BulkStringValue("SET"),
			BulkStringValue("key"),
			BulkStringValue("value"),
		),
		ArrayValue(
			IntegerValue(1),
			BulkStringValue("test"),
			ArrayValue(
				SimpleStringValue("nested"),
			),
		),
	}

	for i, original := range tests {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			buf := new(bytes.Buffer)
			s := NewSerializer(buf)
			err := s.Serialize(original)
			if err != nil {
				t.Fatalf("Serialize() error = %v", err)
			}

			p := NewParser(bytes.NewReader(buf.Bytes()))
			parsed, err := p.Parse()
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			if !valuesEqual(original, parsed) {
				t.Errorf("Round trip failed: original = %+v, parsed = %+v", original, parsed)
			}
		})
	}
}

func TestHelperFunctions(t *testing.T) {
	ok := OKValue()
	if ok.Type != SimpleString || ok.Str != "OK" {
		t.Errorf("OKValue() = %+v, want SimpleString 'OK'", ok)
	}

	pong := PongValue()
	if pong.Type != SimpleString || pong.Str != "PONG" {
		t.Errorf("PongValue() = %+v, want SimpleString 'PONG'", pong)
	}
}
