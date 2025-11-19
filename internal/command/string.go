package command

import (
	"strconv"
	"strings"
	"time"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func SetCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.ErrorValue("ERR wrong number of arguments for 'set' command")
		}

		if args[0].Type != resp.BulkString || args[1].Type != resp.BulkString {
			return resp.ErrorValue("ERR invalid argument type")
		}

		key := args[0].Str
		value := args[1].Str

		nx := false
		xx := false
		var expiry *time.Time

		for i := 2; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.ErrorValue("ERR invalid argument type")
			}

			option := strings.ToUpper(args[i].Str)

			switch option {
			case "NX":
				nx = true
			case "XX":
				xx = true
			case "EX":

				if i+1 >= len(args) {
					return resp.ErrorValue("ERR syntax error")
				}
				i++
				if args[i].Type != resp.BulkString {
					return resp.ErrorValue("ERR invalid argument type")
				}
				seconds, err := strconv.ParseInt(args[i].Str, 10, 64)
				if err != nil || seconds <= 0 {
					return resp.ErrorValue("ERR invalid expire time in 'set' command")
				}
				expiryTime := time.Now().Add(time.Duration(seconds) * time.Second)
				expiry = &expiryTime

			case "PX":

				if i+1 >= len(args) {
					return resp.ErrorValue("ERR syntax error")
				}
				i++
				if args[i].Type != resp.BulkString {
					return resp.ErrorValue("ERR invalid argument type")
				}
				millis, err := strconv.ParseInt(args[i].Str, 10, 64)
				if err != nil || millis <= 0 {
					return resp.ErrorValue("ERR invalid expire time in 'set' command")
				}
				expiryTime := time.Now().Add(time.Duration(millis) * time.Millisecond)
				expiry = &expiryTime

			default:
				return resp.ErrorValue("ERR syntax error")
			}
		}

		if nx && xx {
			return resp.ErrorValue("ERR syntax error")
		}

		if nx {
			success, err := s.SetNX(key, value)
			if err != nil {
				return resp.ErrorValue(err.Error())
			}
			if !success {
				return resp.NullBulkStringValue()
			}
			if expiry != nil {
				if err := s.SetWithExpiry(key, value, *expiry); err != nil {
					return resp.ErrorValue(err.Error())
				}
			}
			return resp.OKValue()
		}

		if xx {
			success, err := s.SetXX(key, value)
			if err != nil {
				return resp.ErrorValue(err.Error())
			}
			if !success {
				return resp.NullBulkStringValue()
			}
			if expiry != nil {
				if err := s.SetWithExpiry(key, value, *expiry); err != nil {
					return resp.ErrorValue(err.Error())
				}
			}
			return resp.OKValue()
		}

		if expiry != nil {
			if err := s.SetWithExpiry(key, value, *expiry); err != nil {
				return resp.ErrorValue(err.Error())
			}
		} else {
			if err := s.Set(key, value); err != nil {
				return resp.ErrorValue(err.Error())
			}
		}

		return resp.OKValue()
	}
}

func GetCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.ErrorValue("ERR wrong number of arguments for 'get' command")
		}

		if args[0].Type != resp.BulkString {
			return resp.ErrorValue("ERR invalid argument type")
		}

		key := args[0].Str

		value, exists := s.Get(key)
		if !exists {
			return resp.NullBulkStringValue()
		}

		return resp.BulkStringValue(value)
	}
}

func DelCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) == 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'del' command")
		}

		count := int64(0)
		for _, arg := range args {
			if arg.Type != resp.BulkString {
				return resp.ErrorValue("ERR invalid argument type")
			}

			if s.Delete(arg.Str) {
				count++
			}
		}

		return resp.IntegerValue(count)
	}
}

func ExistsCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) == 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'exists' command")
		}

		count := int64(0)
		for _, arg := range args {
			if arg.Type != resp.BulkString {
				return resp.ErrorValue("ERR invalid argument type")
			}

			if s.Exists(arg.Str) {
				count++
			}
		}

		return resp.IntegerValue(count)
	}
}

func TypeCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.ErrorValue("ERR wrong number of arguments for 'type' command")
		}

		if args[0].Type != resp.BulkString {
			return resp.ErrorValue("ERR invalid argument type")
		}

		key := args[0].Str

		objType, exists := s.GetType(key)
		if !exists {
			return resp.SimpleStringValue("none")
		}

		var typeName string
		switch objType {
		case store.ObjString:
			typeName = "string"
		case store.ObjList:
			typeName = "list"
		case store.ObjHash:
			typeName = "hash"
		case store.ObjSet:
			typeName = "set"
		case store.ObjZSet:
			typeName = "zset"
		default:
			typeName = "unknown"
		}

		return resp.SimpleStringValue(typeName)
	}
}

func IncrCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.ErrorValue("ERR wrong number of arguments for 'incr' command")
		}

		if args[0].Type != resp.BulkString {
			return resp.ErrorValue("ERR invalid argument type")
		}

		key := args[0].Str

		value, exists := s.Get(key)
		var num int64

		if exists {

			var err error
			num, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return resp.ErrorValue("ERR value is not an integer or out of range")
			}
		} else {
			num = 0
		}

		num++

		s.Set(key, strconv.FormatInt(num, 10))

		return resp.IntegerValue(num)
	}
}

func DecrCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.ErrorValue("ERR wrong number of arguments for 'decr' command")
		}

		if args[0].Type != resp.BulkString {
			return resp.ErrorValue("ERR invalid argument type")
		}

		key := args[0].Str

		value, exists := s.Get(key)
		var num int64

		if exists {

			var err error
			num, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return resp.ErrorValue("ERR value is not an integer or out of range")
			}
		} else {
			num = 0
		}

		num--

		s.Set(key, strconv.FormatInt(num, 10))

		return resp.IntegerValue(num)
	}
}
