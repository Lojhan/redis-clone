package command

import (
	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func HSetCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 3 || len(args)%2 == 0 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hset' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		added := int64(0)
		for i := 1; i < len(args); i += 2 {
			if args[i].Type != resp.BulkString || args[i+1].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid field or value type",
				}
			}

			field := args[i].Str
			value := args[i+1].Str

			count, err := s.HSet(key, field, value)
			if err != nil {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR " + err.Error(),
				}
			}
			added += count
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  added,
		}
	}
}

func HGetCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hget' command",
			}
		}

		if args[0].Type != resp.BulkString || args[1].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid argument type",
			}
		}

		key := args[0].Str
		field := args[1].Str

		value, exists := s.HGet(key, field)
		if !exists {
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

func HDelCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hdel' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		fields := make([]string, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid field type",
				}
			}
			fields = append(fields, args[i].Str)
		}

		count, err := s.HDel(key, fields...)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  count,
		}
	}
}

func HExistsCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hexists' command",
			}
		}

		if args[0].Type != resp.BulkString || args[1].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid argument type",
			}
		}

		key := args[0].Str
		field := args[1].Str

		exists := s.HExists(key, field)
		if exists {
			return resp.Value{
				Type: resp.Integer,
				Int:  1,
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  0,
		}
	}
}

func HLenCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hlen' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		length, err := s.HLen(key)
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

func HGetAllCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hgetall' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		fieldValues, err := s.HGetAll(key)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		result := make([]resp.Value, 0, len(fieldValues)*2)
		for field, value := range fieldValues {
			result = append(result, resp.Value{
				Type: resp.BulkString,
				Str:  field,
			})
			result = append(result, resp.Value{
				Type: resp.BulkString,
				Str:  value,
			})
		}

		return resp.Value{
			Type:  resp.Array,
			Array: result,
		}
	}
}

func HKeysCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hkeys' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		fields, err := s.HKeys(key)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		result := make([]resp.Value, len(fields))
		for i, field := range fields {
			result[i] = resp.Value{
				Type: resp.BulkString,
				Str:  field,
			}
		}

		return resp.Value{
			Type:  resp.Array,
			Array: result,
		}
	}
}

func HValsCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'hvals' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		values, err := s.HVals(key)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		result := make([]resp.Value, len(values))
		for i, value := range values {
			result[i] = resp.Value{
				Type: resp.BulkString,
				Str:  value,
			}
		}

		return resp.Value{
			Type:  resp.Array,
			Array: result,
		}
	}
}
