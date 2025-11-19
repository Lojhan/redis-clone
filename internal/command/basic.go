package command

import (
	"strings"

	"github.com/lojhan/redis-clone/internal/resp"
)

func PingCommand(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.PongValue()
	}

	if len(args) > 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'ping' command")
	}

	if args[0].Type != resp.BulkString {
		return resp.ErrorValue("ERR invalid argument type")
	}

	return args[0]
}

func EchoCommand(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}

	if args[0].Type != resp.BulkString {
		return resp.ErrorValue("ERR invalid argument type")
	}

	return args[0]
}

func CommandCommand(args []resp.Value) resp.Value {

	return resp.ArrayValue()
}

func InfoCommand(args []resp.Value) resp.Value {
	section := "server"
	if len(args) > 0 {
		if args[0].Type == resp.BulkString {
			section = strings.ToLower(args[0].Str)
		}
	}

	info := ""
	switch section {
	case "server":
		info = "# Server\r\n"
		info += "redis_version:7.0.0-clone\r\n"
		info += "redis_mode:standalone\r\n"
		info += "os:Go\r\n"
		info += "arch_bits:64\r\n"
	case "clients":
		info = "# Clients\r\n"
		info += "connected_clients:1\r\n"
	default:

		info = "# Server\r\n"
		info += "redis_version:7.0.0-clone\r\n"
	}

	return resp.BulkStringValue(info)
}

func ConfigCommand(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'config' command")
	}

	if args[0].Type != resp.BulkString {
		return resp.ErrorValue("ERR invalid argument type")
	}

	subcommand := strings.ToUpper(args[0].Str)

	switch subcommand {
	case "GET":
		if len(args) < 2 {
			return resp.ErrorValue("ERR wrong number of arguments for 'config|get' command")
		}

		return resp.ArrayValue()
	case "SET":
		if len(args) < 3 {
			return resp.ErrorValue("ERR wrong number of arguments for 'config|set' command")
		}
		return resp.OKValue()
	default:
		return resp.ErrorValue("ERR unknown subcommand '" + subcommand + "'. Try CONFIG HELP.")
	}
}
