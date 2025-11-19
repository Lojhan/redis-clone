package command

import (
	"github.com/lojhan/redis-clone/internal/resp"
)

func MultiCommand() func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'multi' command",
			}
		}

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}
}

func ExecCommand() func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'exec' command",
			}
		}

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "EXEC",
		}
	}
}

func DiscardCommand() func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.Value{
				Type: resp.Error,
				Str:  "ERR wrong number of arguments for 'discard' command",
			}
		}

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}
}
