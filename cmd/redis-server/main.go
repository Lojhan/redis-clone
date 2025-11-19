package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/lojhan/redis-clone/internal/command"
	"github.com/lojhan/redis-clone/internal/persistence"
	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/server"
	"github.com/lojhan/redis-clone/internal/store"
)

func main() {
	port := flag.String("port", "6379", "Port to listen on")
	rdbFile := flag.String("dbfilename", "dump.rdb", "RDB file name")
	aofFile := flag.String("appendfilename", "appendonly.aof", "AOF file name")
	useAof := flag.Bool("appendonly", false, "Enable AOF persistence")
	aofSyncPolicy := flag.String("appendfsync", "everysec", "AOF fsync policy: always, everysec, no")
	maxMemory := flag.Int64("maxmemory", 0, "Maximum memory in bytes (0 = no limit)")
	maxMemoryPolicy := flag.String("maxmemory-policy", "noeviction", "Eviction policy: noeviction, allkeys-lru, volatile-lru, allkeys-random, volatile-random, volatile-ttl")
	maxMemorySamples := flag.Int("maxmemory-samples", 5, "Number of samples for approximate LRU")
	flag.Parse()

	srv := server.NewServer()
	dataStore := store.NewStore()

	if *maxMemory > 0 {
		evictionConfig := store.NewEvictionConfig(
			*maxMemory,
			store.EvictionPolicy(*maxMemoryPolicy),
			*maxMemorySamples,
		)
		dataStore.SetEvictionConfig(evictionConfig)
		log.Printf("Memory eviction enabled: maxmemory=%d bytes, policy=%s, samples=%d",
			*maxMemory, *maxMemoryPolicy, *maxMemorySamples)
	}

	dataStore.SetKeyModifiedHandler(srv.MarkKeyModified)

	srv.RegisterCommand("PING", command.PingCommand)
	srv.RegisterCommand("ECHO", command.EchoCommand)
	srv.RegisterCommand("COMMAND", command.CommandCommand)
	srv.RegisterCommand("INFO", command.InfoCommand)
	srv.RegisterCommand("CONFIG", command.ConfigCommand)

	srv.RegisterCommand("SET", command.SetCommand(dataStore))
	srv.RegisterCommand("GET", command.GetCommand(dataStore))
	srv.RegisterCommand("DEL", command.DelCommand(dataStore))
	srv.RegisterCommand("EXISTS", command.ExistsCommand(dataStore))
	srv.RegisterCommand("TYPE", command.TypeCommand(dataStore))
	srv.RegisterCommand("INCR", command.IncrCommand(dataStore))
	srv.RegisterCommand("DECR", command.DecrCommand(dataStore))

	srv.RegisterCommand("LPUSH", command.LPushCommand(dataStore))
	srv.RegisterCommand("RPUSH", command.RPushCommand(dataStore))
	srv.RegisterCommand("LPOP", command.LPopCommand(dataStore))
	srv.RegisterCommand("RPOP", command.RPopCommand(dataStore))
	srv.RegisterCommand("LLEN", command.LLenCommand(dataStore))
	srv.RegisterCommand("LRANGE", command.LRangeCommand(dataStore))

	srv.RegisterCommand("HSET", command.HSetCommand(dataStore))
	srv.RegisterCommand("HGET", command.HGetCommand(dataStore))
	srv.RegisterCommand("HDEL", command.HDelCommand(dataStore))
	srv.RegisterCommand("HEXISTS", command.HExistsCommand(dataStore))
	srv.RegisterCommand("HLEN", command.HLenCommand(dataStore))
	srv.RegisterCommand("HGETALL", command.HGetAllCommand(dataStore))
	srv.RegisterCommand("HKEYS", command.HKeysCommand(dataStore))
	srv.RegisterCommand("HVALS", command.HValsCommand(dataStore))

	srv.RegisterCommand("SADD", command.SAddCommand(dataStore))
	srv.RegisterCommand("SREM", command.SRemCommand(dataStore))
	srv.RegisterCommand("SISMEMBER", command.SIsMemberCommand(dataStore))
	srv.RegisterCommand("SMEMBERS", command.SMembersCommand(dataStore))
	srv.RegisterCommand("SCARD", command.SCardCommand(dataStore))
	srv.RegisterCommand("SPOP", command.SPopCommand(dataStore))

	srv.RegisterCommand("ZADD", command.ZAddCommand(dataStore))
	srv.RegisterCommand("ZREM", command.ZRemCommand(dataStore))
	srv.RegisterCommand("ZSCORE", command.ZScoreCommand(dataStore))
	srv.RegisterCommand("ZCARD", command.ZCardCommand(dataStore))
	srv.RegisterCommand("ZRANK", command.ZRankCommand(dataStore))
	srv.RegisterCommand("ZRANGE", command.ZRangeCommand(dataStore))

	srv.RegisterCommand("SAVE", command.SaveCommand(dataStore))
	srv.RegisterCommand("BGSAVE", command.BGSaveCommand(dataStore))
	srv.RegisterCommand("LASTSAVE", command.LastSaveCommand())
	srv.RegisterCommand("BGREWRITEAOF", command.BGRewriteAOFCommand(dataStore))
	srv.RegisterCommand("SHUTDOWN", command.ShutdownCommand(dataStore))
	srv.RegisterCommand("DBSIZE", command.DBSizeCommand(dataStore))
	srv.RegisterCommand("FLUSHDB", command.FlushDBCommand(dataStore))
	srv.RegisterCommand("FLUSHALL", command.FlushAllCommand(dataStore))

	if *useAof {
		log.Printf("Loading AOF file: %s", *aofFile)

		executeCommand := func(values []resp.Value) resp.Value {
			if len(values) == 0 {
				return resp.Value{Type: resp.Error, Str: "ERR empty command"}
			}

			cmdName := values[0].Str
			handler := srv.GetHandler(cmdName)
			if handler == nil {
				return resp.Value{Type: resp.Error, Str: "ERR unknown command"}
			}

			args := values[1:]
			return handler(args)
		}

		if err := persistence.LoadAOF(*aofFile, dataStore, executeCommand); err != nil {
			log.Printf("Warning: Failed to load AOF file: %v", err)
		} else {
			keyCount := len(dataStore.Keys())
			if keyCount > 0 {
				log.Printf("Loaded %d keys from AOF file", keyCount)
			} else {
				log.Println("No data found in AOF file")
			}
		}
	} else {

		log.Printf("Loading RDB file: %s", *rdbFile)
		if err := persistence.LoadRDB(*rdbFile, dataStore); err != nil {
			log.Printf("Warning: Failed to load RDB file: %v", err)
		} else {
			keyCount := len(dataStore.Keys())
			if keyCount > 0 {
				log.Printf("Loaded %d keys from RDB file", keyCount)
			} else {
				log.Println("No data found in RDB file, starting with empty database")
			}
		}
	}

	if *useAof {

		policy := persistence.AOFSyncAlways
		if *aofSyncPolicy == "everysec" {
			policy = persistence.AOFSyncEverySec
		} else if *aofSyncPolicy == "no" {
			policy = persistence.AOFSyncNo
		}

		aof, err := persistence.NewAOFWriter(*aofFile, policy)
		if err != nil {
			log.Fatalf("Failed to create AOF writer: %v", err)
		}
		srv.SetAOFWriter(aof)
		log.Printf("AOF logging enabled (sync policy: %s)", *aofSyncPolicy)

		defer func() {
			if err := aof.Close(); err != nil {
				log.Printf("Error closing AOF: %v", err)
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("\nShutting down server...")
		if err := srv.Stop(); err != nil {
			log.Printf("Error stopping server: %v", err)
		}
		os.Exit(0)
	}()

	log.Printf("Starting Redis clone server on port %s", *port)
	if err := srv.Start(*port); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
