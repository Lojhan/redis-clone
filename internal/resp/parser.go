package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

var (
	ErrInvalidType   = errors.New("invalid RESP type")
	ErrInvalidFormat = errors.New("invalid RESP format")
)

type Value struct {
	Type  Type
	Str   string
	Int   int64
	Array []Value
	Null  bool
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(r),
	}
}

func (p *Parser) Parse() (Value, error) {
	typeByte, err := p.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch Type(typeByte) {
	case SimpleString:
		return p.parseSimpleString()
	case Error:
		return p.parseError()
	case Integer:
		return p.parseInteger()
	case BulkString:
		return p.parseBulkString()
	case Array:
		return p.parseArray()
	default:
		return Value{}, fmt.Errorf("%w: %c", ErrInvalidType, typeByte)
	}
}

func (p *Parser) parseSimpleString() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Type: SimpleString, Str: line}, nil
}

func (p *Parser) parseError() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return Value{}, err
	}
	return Value{Type: Error, Str: line}, nil
}

func (p *Parser) parseInteger() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return Value{}, err
	}

	num, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return Value{}, fmt.Errorf("%w: invalid integer", ErrInvalidFormat)
	}

	return Value{Type: Integer, Int: num}, nil
}

func (p *Parser) parseBulkString() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, fmt.Errorf("%w: invalid bulk string length", ErrInvalidFormat)
	}

	if length == -1 {
		return Value{Type: BulkString, Null: true}, nil
	}

	if length == 0 {

		_, err := p.readLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Type: BulkString, Str: ""}, nil
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(p.reader, buf)
	if err != nil {
		return Value{}, err
	}

	crlf := make([]byte, 2)
	_, err = io.ReadFull(p.reader, crlf)
	if err != nil {
		return Value{}, err
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return Value{}, fmt.Errorf("%w: missing CRLF after bulk string", ErrInvalidFormat)
	}

	return Value{Type: BulkString, Str: string(buf)}, nil
}

func (p *Parser) parseArray() (Value, error) {
	line, err := p.readLine()
	if err != nil {
		return Value{}, err
	}

	count, err := strconv.Atoi(line)
	if err != nil {
		return Value{}, fmt.Errorf("%w: invalid array length", ErrInvalidFormat)
	}

	if count == -1 {
		return Value{Type: Array, Null: true}, nil
	}

	if count == 0 {
		return Value{Type: Array, Array: []Value{}}, nil
	}

	array := make([]Value, count)
	for i := range count {
		val, err := p.Parse()
		if err != nil {
			return Value{}, err
		}
		array[i] = val
	}

	return Value{Type: Array, Array: array}, nil
}

func (p *Parser) readLine() (string, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return "", err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' || line[len(line)-1] != '\n' {
		return "", fmt.Errorf("%w: missing CRLF", ErrInvalidFormat)
	}

	return line[:len(line)-2], nil
}
