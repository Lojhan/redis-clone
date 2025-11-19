package command

import (
	"strconv"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func ZAddCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 3 || len(args)%2 == 0 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'zadd' command",
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
					Str:  "ERR invalid score or member type",
				}
			}

			scoreStr := args[i].Str
			member := args[i+1].Str

			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR value is not a valid float",
				}
			}

			count, err := s.ZAdd(key, score, member)
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

func ZRemCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'zrem' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		removed := int64(0)
		for i := 1; i < len(args); i++ {
			if args[i].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR invalid member type",
				}
			}

			member := args[i].Str

			count, err := s.ZRem(key, member)
			if err != nil {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR " + err.Error(),
				}
			}
			removed += count
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  removed,
		}
	}
}

func ZScoreCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'zscore' command",
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

		score, exists := s.ZScore(key, member)
		if !exists {
			return resp.Value{
				Type: resp.BulkString,
				Null: true,
			}
		}

		return resp.Value{
			Type: resp.BulkString,
			Str:  strconv.FormatFloat(score, 'f', -1, 64),
		}
	}
}

func ZCardCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 1 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'zcard' command",
			}
		}

		if args[0].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid key type",
			}
		}

		key := args[0].Str

		card, err := s.ZCard(key)
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

func ZRankCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 2 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'zrank' command",
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

		rank, exists := s.ZRank(key, member)
		if !exists {
			return resp.Value{
				Type: resp.BulkString,
				Null: true,
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  rank,
		}
	}
}

func ZRangeCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) < 3 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'zrange' command",
			}
		}

		if args[0].Type != resp.BulkString || args[1].Type != resp.BulkString || args[2].Type != resp.BulkString {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR invalid argument type",
			}
		}

		key := args[0].Str

		start, err := strconv.ParseInt(args[1].Str, 10, 64)
		if err != nil {
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

		withScores := false
		if len(args) == 4 {
			if args[3].Type != resp.BulkString {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR syntax error",
				}
			}
			if args[3].Str == "WITHSCORES" {
				withScores = true
			} else {
				return resp.Value{
					Type: resp.Error,
					Str:  "ERR syntax error",
				}
			}
		}

		members, err := s.ZRange(key, start, stop)
		if err != nil {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR " + err.Error(),
			}
		}

		var result []resp.Value
		if withScores {
			result = make([]resp.Value, len(members)*2)
			for i, zm := range members {
				result[i*2] = resp.Value{
					Type: resp.BulkString,
					Str:  zm.Member,
				}
				result[i*2+1] = resp.Value{
					Type: resp.BulkString,
					Str:  strconv.FormatFloat(zm.Score, 'f', -1, 64),
				}
			}
		} else {
			result = make([]resp.Value, len(members))
			for i, zm := range members {
				result[i] = resp.Value{
					Type: resp.BulkString,
					Str:  zm.Member,
				}
			}
		}

		return resp.Value{
			Type:  resp.Array,
			Array: result,
		}
	}
}
