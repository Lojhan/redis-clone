package command

import (
	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func SAddCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'sadd' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid member type",
				}
			}
			members[i-1] = args[i].Str
		}

		added, err := s.SAdd(key, members...)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  added,
		}
	}
}

func SRemCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'srem' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		members := make([]string, len(args)-1)
		for i := 1; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid member type",
				}
			}
			members[i-1] = args[i].Str
		}

		removed, err := s.SRem(key, members...)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  removed,
		}
	}
}

func SIsMemberCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'sismember' command",
			}
		}

		if args[0].Type != resp.BulkString || args[1].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid argument type",
			}
		}

		key := args[0].Str
		member := args[1].Str

		isMember := s.SIsMember(key, member)

		intVal := int64(0)
		if isMember {
			intVal = 1
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  intVal,
		}
	}
}

func SMembersCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'smembers' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		members, err := s.SMembers(key)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		values := make([]resp.Value, len(members))
		for i, member := range members {
			values[i] = resp.Value{
				Type: resp.BulkString,
				Str:  member,
			}
		}

		return resp.Value{
			Type:  resp.Array,
			Array: values,
		}
	}
}

func SCardCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'scard' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		card, err := s.SCard(key)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  card,
		}
	}
}

func SPopCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'spop' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		member, ok := s.SPop(key)
		if !ok {
			return resp.Value{
				Type: resp.BulkString,
				Null: true,
			}
		}

		return resp.Value{
			Type: resp.BulkString,
			Str:  member,
		}
	}
}
