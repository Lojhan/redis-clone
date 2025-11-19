package command

import (
	"strconv"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func LPushCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'lpush' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		values := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid value type",
				}
			}
			values = append(values, args[i].Str)
		}

		length, err := s.LPush(key, values...)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  length,
		}
	}
}

func RPushCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'rpush' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		values := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid value type",
				}
			}
			values = append(values, args[i].Str)
		}

		length, err := s.RPush(key, values...)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  length,
		}
	}
}

func LPopCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'lpop' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		value, ok := s.LPop(key)
		if !ok {
			return resp.Value{
				Type: resp.BulkString,
				Null: true,
			}
		}

		return resp.Value{
			Type: resp.BulkString,
			Str:  value,
		}
	}
}

func RPopCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'rpop' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		value, ok := s.RPop(key)
		if !ok {
			return resp.Value{
				Type: resp.BulkString,
				Null: true,
			}
		}

		return resp.Value{
			Type: resp.BulkString,
			Str:  value,
		}
	}
}

func LLenCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'llen' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		length, err := s.LLen(key)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  length,
		}
	}
}

func LRangeCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 3 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'lrange' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		if args[1].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR value is not an integer or out of range",
			}
		}
		start, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR value is not an integer or out of range",
			}
		}

		if args[2].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR value is not an integer or out of range",
			}
		}
		stop, err := strconv.ParseInt(args[2].Str, 10, 64)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR value is not an integer or out of range",
			}
		}

		values, err := s.LRange(key, start, stop)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		result := make([]resp.Value, len(values))
		for i, v := range values {
			result[i] = resp.Value{
				Type: resp.BulkString,
				Str:  v,
			}
		}

		return resp.Value{
			Type:  resp.Array,
			Array: result,
		}
	}
}
