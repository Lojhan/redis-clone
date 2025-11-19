package persistence

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

type AOFSyncPolicy string

const (
	AOFSyncAlways   AOFSyncPolicy = "always"
	AOFSyncEverySec AOFSyncPolicy = "everysec"
	AOFSyncNo       AOFSyncPolicy = "no"
)

type AOFWriter struct {
	file       *os.File
	writer     *bufio.Writer
	mu         sync.Mutex
	syncPolicy AOFSyncPolicy
	lastSync   time.Time
	stopChan   chan struct{}
	syncTicker *time.Ticker
}

func NewAOFWriter(filepath string, policy AOFSyncPolicy) (*AOFWriter, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open AOF file: %w", err)
	}

	aof := &AOFWriter{
		file:       file,
		writer:     bufio.NewWriter(file),
		syncPolicy: policy,
		lastSync:   time.Now(),
		stopChan:   make(chan struct{}),
	}

	if policy == AOFSyncEverySec {
		aof.syncTicker = time.NewTicker(1 * time.Second)
		go aof.backgroundSync()
	}

	return aof, nil
}

func (a *AOFWriter) Append(command []resp.Value) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	data := resp.SerializeArray(command)

	if _, err := a.writer.Write(data); err != nil {
		return fmt.Errorf("failed to write to AOF buffer: %w", err)
	}

	switch a.syncPolicy {
	case AOFSyncAlways:

		if err := a.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush AOF buffer: %w", err)
		}
		if err := a.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync AOF to disk: %w", err)
		}
		a.lastSync = time.Now()
	case AOFSyncEverySec:

		if err := a.writer.Flush(); err != nil {
			return fmt.Errorf("failed to flush AOF buffer: %w", err)
		}
	case AOFSyncNo:

	}

	return nil
}

func (a *AOFWriter) backgroundSync() {
	for {
		select {
		case <-a.syncTicker.C:
			a.mu.Lock()

			a.writer.Flush()
			a.file.Sync()
			a.lastSync = time.Now()
			a.mu.Unlock()
		case <-a.stopChan:
			return
		}
	}
}

func (a *AOFWriter) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.syncTicker != nil {
		a.syncTicker.Stop()
		close(a.stopChan)
	}

	if err := a.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF on close: %w", err)
	}
	if err := a.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync AOF on close: %w", err)
	}
	if err := a.file.Close(); err != nil {
		return fmt.Errorf("failed to close AOF file: %w", err)
	}

	return nil
}

func LoadAOF(filepath string, st *store.Store, executeCommand func([]resp.Value) resp.Value) error {
	file, err := os.Open(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to open AOF file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	parser := resp.NewParser(reader)

	commandCount := 0
	for {
		value, err := parser.Parse()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to parse AOF at command %d: %w", commandCount+1, err)
		}

		if value.Type != resp.Array {
			return fmt.Errorf("invalid AOF entry at command %d: expected array, got %c", commandCount+1, value.Type)
		}

		result := executeCommand(value.Array)

		if result.Type == resp.Error {

		}

		commandCount++
	}

	return nil
}

func RewriteAOF(filepath string, st *store.Store) error {

	tmpFile := filepath + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp AOF file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	data, expires := st.Snapshot()

	for key, obj := range data {
		if err := writeObjectAsCommands(writer, key, obj, expires); err != nil {
			return fmt.Errorf("failed to write object %s: %w", key, err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush AOF: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync AOF: %w", err)
	}

	file.Close()

	if err := os.Rename(tmpFile, filepath); err != nil {
		return fmt.Errorf("failed to rename AOF file: %w", err)
	}

	return nil
}

func writeObjectAsCommands(w *bufio.Writer, key string, obj *store.RedisObject, expires map[string]time.Time) error {
	switch obj.Type {
	case store.ObjString:

		value := extractStringValue(obj)
		cmd := resp.SerializeArray([]resp.Value{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: key},
			{Type: resp.BulkString, Str: value},
		})
		if _, err := w.Write(cmd); err != nil {
			return err
		}

	case store.ObjList:

		list, ok := obj.Ptr.(*store.Quicklist)
		if !ok {
			return fmt.Errorf("invalid list encoding")
		}
		values := list.ToSlice()
		if len(values) > 0 {

			cmdValues := []resp.Value{
				{Type: resp.BulkString, Str: "RPUSH"},
				{Type: resp.BulkString, Str: key},
			}
			for _, v := range values {
				cmdValues = append(cmdValues, resp.Value{Type: resp.BulkString, Str: v})
			}
			cmd := resp.SerializeArray(cmdValues)
			if _, err := w.Write(cmd); err != nil {
				return err
			}
		}

	case store.ObjHash:

		hash, ok := obj.Ptr.(*store.HashTable)
		if !ok {
			return fmt.Errorf("invalid hash encoding")
		}
		fields := hash.GetAll()
		if len(fields) > 0 {

			cmdValues := []resp.Value{
				{Type: resp.BulkString, Str: "HSET"},
				{Type: resp.BulkString, Str: key},
			}
			for field, value := range fields {
				cmdValues = append(cmdValues,
					resp.Value{Type: resp.BulkString, Str: field},
					resp.Value{Type: resp.BulkString, Str: value},
				)
			}
			cmd := resp.SerializeArray(cmdValues)
			if _, err := w.Write(cmd); err != nil {
				return err
			}
		}

	case store.ObjSet:

		set, ok := obj.Ptr.(*store.Set)
		if !ok {
			return fmt.Errorf("invalid set encoding")
		}
		members := set.Members()
		if len(members) > 0 {

			cmdValues := []resp.Value{
				{Type: resp.BulkString, Str: "SADD"},
				{Type: resp.BulkString, Str: key},
			}
			for _, member := range members {
				cmdValues = append(cmdValues, resp.Value{Type: resp.BulkString, Str: member})
			}
			cmd := resp.SerializeArray(cmdValues)
			if _, err := w.Write(cmd); err != nil {
				return err
			}
		}

	case store.ObjZSet:

		zset, ok := obj.Ptr.(*store.ZSet)
		if !ok {
			return fmt.Errorf("invalid zset encoding")
		}
		members := zset.Range(0, int64(zset.Card()-1))
		if len(members) > 0 {

			cmdValues := []resp.Value{
				{Type: resp.BulkString, Str: "ZADD"},
				{Type: resp.BulkString, Str: key},
			}
			for _, member := range members {
				scoreValue := resp.Value{Type: resp.BulkString, Str: fmt.Sprintf("%g", member.Score)}
				memberValue := resp.Value{Type: resp.BulkString, Str: member.Member}
				cmdValues = append(cmdValues, scoreValue, memberValue)
			}
			cmd := resp.SerializeArray(cmdValues)
			if _, err := w.Write(cmd); err != nil {
				return err
			}
		}
	}

	if expiry, ok := expires[key]; ok {

		ms := expiry.UnixMilli()
		cmd := resp.SerializeArray([]resp.Value{
			{Type: resp.BulkString, Str: "PEXPIREAT"},
			{Type: resp.BulkString, Str: key},
			{Type: resp.BulkString, Str: fmt.Sprintf("%d", ms)},
		})
		if _, err := w.Write(cmd); err != nil {
			return err
		}
	}

	return nil
}

func extractStringValue(obj *store.RedisObject) string {
	switch obj.Encoding {
	case store.EncodingInt:
		return fmt.Sprintf("%d", obj.Ptr.(int64))
	case store.EncodingEmbstr, store.EncodingRaw:
		return obj.Ptr.(string)
	default:
		return ""
	}
}
