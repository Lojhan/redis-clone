package command

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"sync"

	"github.com/lojhan/redis-clone/internal/persistence"
	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

const DefaultRDBFile = "dump.rdb"

var (
	bgSaveMu      sync.Mutex
	bgSaveRunning bool
)

func SaveCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'save' command")
		}

		if err := persistence.SaveRDB(DefaultRDBFile, s); err != nil {
			log.Printf("SAVE failed: %v", err)
			return resp.ErrorValue(fmt.Sprintf("ERR save failed: %v", err))
		}

		log.Println("DB saved on disk")
		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}
}

func BGSaveCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'bgsave' command")
		}

		bgSaveMu.Lock()
		if bgSaveRunning {
			bgSaveMu.Unlock()
			return resp.ErrorValue("ERR Background save already in progress")
		}
		bgSaveRunning = true
		bgSaveMu.Unlock()

		go func() {
			defer func() {
				bgSaveMu.Lock()
				bgSaveRunning = false
				bgSaveMu.Unlock()
			}()

			data, expires := s.Snapshot()

			tempStore := store.NewStore()
			tempStore.RestoreSnapshot(data, expires)

			if err := persistence.SaveRDB(DefaultRDBFile, tempStore); err != nil {
				log.Printf("Background save failed: %v", err)
			} else {
				log.Println("Background saving completed successfully")
			}
		}()

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "Background saving started",
		}
	}
}

func LastSaveCommand() func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'lastsave' command")
		}

		info, err := os.Stat(DefaultRDBFile)
		if err != nil {

			return resp.Value{
				Type: resp.Integer,
				Int:  0,
			}
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  info.ModTime().Unix(),
		}
	}
}

var (
	bgRewriteMu      sync.Mutex
	bgRewriteRunning bool
)

const DefaultAOFFile = "appendonly.aof"

func BGRewriteAOFCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'bgrewriteaof' command")
		}

		bgRewriteMu.Lock()
		if bgRewriteRunning {
			bgRewriteMu.Unlock()
			return resp.ErrorValue("ERR Background AOF rewrite already in progress")
		}
		bgRewriteRunning = true
		bgRewriteMu.Unlock()

		go func() {
			defer func() {
				bgRewriteMu.Lock()
				bgRewriteRunning = false
				bgRewriteMu.Unlock()
			}()

			if err := persistence.RewriteAOF(DefaultAOFFile, s); err != nil {
				log.Printf("Background AOF rewrite failed: %v", err)
			} else {
				log.Println("Background AOF rewrite completed successfully")
			}
		}()

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "Background append only file rewriting started",
		}
	}
}

func ShutdownCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {

		save := true
		if len(args) > 0 {
			arg := args[0].Str
			if arg == "NOSAVE" {
				save = false
			} else if arg != "SAVE" {
				return resp.ErrorValue("ERR syntax error")
			}
		}

		if save {
			log.Println("Saving DB before shutdown...")
			if err := persistence.SaveRDB(DefaultRDBFile, s); err != nil {
				log.Printf("Warning: Failed to save DB: %v", err)
			} else {
				log.Println("DB saved")
			}
		}

		log.Println("Server shutting down...")
		go func() {
			os.Exit(0)
		}()

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}
}

func DBSizeCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		if len(args) != 0 {
			return resp.ErrorValue("ERR wrong number of arguments for 'dbsize' command")
		}

		return resp.Value{
			Type: resp.Integer,
			Int:  int64(s.Size()),
		}
	}
}

func FlushDBCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {

		s.FlushDB()

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}
}

func FlushAllCommand(s *store.Store) func([]resp.Value) resp.Value {
	return func(args []resp.Value) resp.Value {
		s.FlushDB()

		return resp.Value{
			Type: resp.SimpleString,
			Str:  "OK",
		}
	}
}

func forkProcess() (int, error) {

	cmd := exec.Command(os.Args[0], "--child-process")
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	return cmd.Process.Pid, nil
}
