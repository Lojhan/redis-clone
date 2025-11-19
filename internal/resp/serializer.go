package resp

import (
	"fmt"
	"io"
	"strconv"
)

type Serializer struct {
	writer io.Writer
}

func NewSerializer(w io.Writer) *Serializer {
	return &Serializer{writer: w}
}

func (s *Serializer) Serialize(v Value) error {
	switch v.Type {
	case SimpleString:
		return s.writeSimpleString(v.Str)
	case Error:
		return s.writeError(v.Str)
	case Integer:
		return s.writeInteger(v.Int)
	case BulkString:
		return s.writeBulkString(v.Str, v.Null)
	case Array:
		return s.writeArray(v.Array, v.Null)
	default:
		return fmt.Errorf("%w: %c", ErrInvalidType, v.Type)
	}
}

func (s *Serializer) writeSimpleString(str string) error {
	_, err := fmt.Fprintf(s.writer, "+%s\r\n", str)
	return err
}

func (s *Serializer) writeError(str string) error {
	_, err := fmt.Fprintf(s.writer, "-%s\r\n", str)
	return err
}

func (s *Serializer) writeInteger(num int64) error {
	_, err := fmt.Fprintf(s.writer, ":%d\r\n", num)
	return err
}

func (s *Serializer) writeBulkString(str string, null bool) error {
	if null {
		_, err := s.writer.Write([]byte("$-1\r\n"))
		return err
	}

	length := len(str)
	_, err := fmt.Fprintf(s.writer, "$%d\r\n%s\r\n", length, str)
	return err
}

func (s *Serializer) writeArray(array []Value, null bool) error {
	if null {
		_, err := s.writer.Write([]byte("*-1\r\n"))
		return err
	}

	_, err := fmt.Fprintf(s.writer, "*%d\r\n", len(array))
	if err != nil {
		return err
	}

	for _, elem := range array {
		if err := s.Serialize(elem); err != nil {
			return err
		}
	}

	return nil
}

func SimpleStringValue(str string) Value {
	return Value{Type: SimpleString, Str: str}
}

func ErrorValue(str string) Value {
	return Value{Type: Error, Str: str}
}

func IntegerValue(num int64) Value {
	return Value{Type: Integer, Int: num}
}

func BulkStringValue(str string) Value {
	return Value{Type: BulkString, Str: str}
}

func NullBulkStringValue() Value {
	return Value{Type: BulkString, Null: true}
}

func ArrayValue(values ...Value) Value {
	return Value{Type: Array, Array: values}
}

func NullArrayValue() Value {
	return Value{Type: Array, Null: true}
}

func OKValue() Value {
	return SimpleStringValue("OK")
}

func PongValue() Value {
	return SimpleStringValue("PONG")
}

func SerializeSimpleString(w io.Writer, str string) error {
	s := NewSerializer(w)
	return s.writeSimpleString(str)
}

func SerializeError(w io.Writer, str string) error {
	s := NewSerializer(w)
	return s.writeError(str)
}

func SerializeInteger(w io.Writer, num int64) error {
	s := NewSerializer(w)
	return s.writeInteger(num)
}

func SerializeBulkString(w io.Writer, str string) error {
	s := NewSerializer(w)
	return s.writeBulkString(str, false)
}

func SerializeNullBulkString(w io.Writer) error {
	s := NewSerializer(w)
	return s.writeBulkString("", true)
}

func SerializeArray(values []Value) []byte {
	var buf []byte
	buf = append(buf, fmt.Sprintf("*%d\r\n", len(values))...)
	for _, v := range values {
		switch v.Type {
		case BulkString:
			if v.Null {
				buf = append(buf, "$-1\r\n"...)
			} else {
				buf = append(buf, fmt.Sprintf("$%d\r\n%s\r\n", len(v.Str), v.Str)...)
			}
		case SimpleString:
			buf = append(buf, fmt.Sprintf("+%s\r\n", v.Str)...)
		case Error:
			buf = append(buf, fmt.Sprintf("-%s\r\n", v.Str)...)
		case Integer:
			buf = append(buf, fmt.Sprintf(":%d\r\n", v.Int)...)
		case Array:

			buf = append(buf, SerializeArray(v.Array)...)
		}
	}
	return buf
}

func CommandToString(cmd string, args ...string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, cmd)
	parts = append(parts, args...)

	result := cmd
	for _, arg := range args {
		result += " " + arg
	}
	return result
}

func ParseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}
