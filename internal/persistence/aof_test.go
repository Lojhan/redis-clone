package persistence

import (
	"os"
	"testing"
	"time"

	"github.com/lojhan/redis-clone/internal/resp"
	"github.com/lojhan/redis-clone/internal/store"
)

func TestAOFAppendAndLoad(t *testing.T) {
	filename := "test_aof.aof"
	defer os.Remove(filename)

	aof, err := NewAOFWriter(filename, AOFSyncAlways)
	if err != nil {
		t.Fatalf("Failed to create AOF writer: %v", err)
	}

	commands := [][]resp.Value{
		{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "key1"},
			{Type: resp.BulkString, Str: "value1"},
		},
		{
			{Type: resp.BulkString, Str: "SET"},
			{Type: resp.BulkString, Str: "key2"},
			{Type: resp.BulkString, Str: "value2"},
		},
		{
			{Type: resp.BulkString, Str: "LPUSH"},
			{Type: resp.BulkString, Str: "list"},
			{Type: resp.BulkString, Str: "a"},
			{Type: resp.BulkString, Str: "b"},
		},
	}

	for _, cmd := range commands {
		if err := aof.Append(cmd); err != nil {
			t.Fatalf("Failed to append command: %v", err)
		}
	}

	if err := aof.Close(); err != nil {
		t.Fatalf("Failed to close AOF: %v", err)
	}

	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("AOF file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("AOF file is empty")
	}

	st := store.NewStore()
	executeCount := 0
	executeCommand := func(values []resp.Value) resp.Value {
		executeCount++
		if len(values) == 0 {
			return resp.Value{Type: resp.Error, Str: "empty command"}
		}

		cmd := values[0].Str
		switch cmd {
		case "SET":
			if len(values) != 3 {
				return resp.Value{Type: resp.Error, Str: "wrong number of arguments"}
			}
			st.Set(values[1].Str, values[2].Str)
			return resp.Value{Type: resp.SimpleString, Str: "OK"}
		case "LPUSH":
			if len(values) < 3 {
				return resp.Value{Type: resp.Error, Str: "wrong number of arguments"}
			}
			args := make([]string, len(values)-2)
			for i := 2; i < len(values); i++ {
				args[i-2] = values[i].Str
			}
			count, _ := st.LPush(values[1].Str, args...)
			return resp.Value{Type: resp.Integer, Int: count}
		default:
			return resp.Value{Type: resp.Error, Str: "unknown command"}
		}
	}

	err = LoadAOF(filename, st, executeCommand)
	if err != nil {
		t.Fatalf("Failed to load AOF: %v", err)
	}

	if executeCount != 3 {
		t.Errorf("Expected 3 commands executed, got %d", executeCount)
	}

	val, ok := st.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("key1: got %v, want value1", val)
	}

	val, ok = st.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("key2: got %v, want value2", val)
	}

	listLen, _ := st.LLen("list")
	if listLen != 2 {
		t.Errorf("list length: got %d, want 2", listLen)
	}
}

func TestAOFSyncPolicies(t *testing.T) {
	tests := []struct {
		name   string
		policy AOFSyncPolicy
	}{
		{"always", AOFSyncAlways},
		{"everysec", AOFSyncEverySec},
		{"no", AOFSyncNo},
	}

	for _, tt := range tests {
		t.Run(string(tt.policy), func(t *testing.T) {
			filename := "test_aof_" + string(tt.policy) + ".aof"
			defer os.Remove(filename)

			aof, err := NewAOFWriter(filename, tt.policy)
			if err != nil {
				t.Fatalf("Failed to create AOF writer: %v", err)
			}

			cmd := []resp.Value{
				{Type: resp.BulkString, Str: "SET"},
				{Type: resp.BulkString, Str: "test"},
				{Type: resp.BulkString, Str: "value"},
			}

			if err := aof.Append(cmd); err != nil {
				t.Fatalf("Failed to append command: %v", err)
			}

			if tt.policy == AOFSyncEverySec {
				time.Sleep(1100 * time.Millisecond)
			}

			if err := aof.Close(); err != nil {
				t.Fatalf("Failed to close AOF: %v", err)
			}

			info, err := os.Stat(filename)
			if err != nil {
				t.Fatalf("AOF file not created: %v", err)
			}
			if info.Size() == 0 {
				t.Fatal("AOF file is empty")
			}
		})
	}
}

func TestAOFRewrite(t *testing.T) {
	filename := "test_rewrite.aof"
	defer os.Remove(filename)

	st := store.NewStore()
	st.Set("string_key", "hello")
	st.RPush("list_key", "a", "b", "c")
	st.HSet("hash_key", "field1", "value1")
	st.HSet("hash_key", "field2", "value2")
	st.SAdd("set_key", "member1", "member2")
	st.ZAdd("zset_key", 1.0, "m1")
	st.ZAdd("zset_key", 2.0, "m2")

	st.SetWithExpiry("expire_key", "test", time.Now().Add(24*time.Hour))

	err := RewriteAOF(filename, st)
	if err != nil {
		t.Fatalf("Failed to rewrite AOF: %v", err)
	}

	info, err := os.Stat(filename)
	if err != nil {
		t.Fatalf("AOF file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("AOF file is empty")
	}
	t.Logf("Rewritten AOF size: %d bytes", info.Size())

	st2 := store.NewStore()
	executeCommand := func(values []resp.Value) resp.Value {
		if len(values) == 0 {
			return resp.Value{Type: resp.Error, Str: "empty command"}
		}

		cmd := values[0].Str
		switch cmd {
		case "SET":
			if len(values) != 3 {
				return resp.Value{Type: resp.Error, Str: "wrong number of arguments"}
			}
			st2.Set(values[1].Str, values[2].Str)
			return resp.Value{Type: resp.SimpleString, Str: "OK"}
		case "RPUSH":
			args := make([]string, len(values)-2)
			for i := 2; i < len(values); i++ {
				args[i-2] = values[i].Str
			}
			count, _ := st2.RPush(values[1].Str, args...)
			return resp.Value{Type: resp.Integer, Int: count}
		case "HSET":
			for i := 2; i < len(values)-1; i += 2 {
				st2.HSet(values[1].Str, values[i].Str, values[i+1].Str)
			}
			return resp.Value{Type: resp.Integer, Int: int64((len(values) - 2) / 2)}
		case "SADD":
			args := make([]string, len(values)-2)
			for i := 2; i < len(values); i++ {
				args[i-2] = values[i].Str
			}
			count, _ := st2.SAdd(values[1].Str, args...)
			return resp.Value{Type: resp.Integer, Int: count}
		case "ZADD":
			for i := 2; i < len(values)-1; i += 2 {
				score, _ := resp.ParseInt64(values[i].Str)
				st2.ZAdd(values[1].Str, float64(score), values[i+1].Str)
			}
			return resp.Value{Type: resp.Integer, Int: int64((len(values) - 2) / 2)}
		case "PEXPIREAT":

			return resp.Value{Type: resp.Integer, Int: 1}
		default:
			return resp.Value{Type: resp.Error, Str: "unknown command"}
		}
	}

	err = LoadAOF(filename, st2, executeCommand)
	if err != nil {
		t.Fatalf("Failed to load AOF: %v", err)
	}

	val, ok := st2.Get("string_key")
	if !ok || val != "hello" {
		t.Errorf("string_key: got %v, want hello", val)
	}

	listLen, _ := st2.LLen("list_key")
	if listLen != 3 {
		t.Errorf("list_key length: got %d, want 3", listLen)
	}

	hashLen, _ := st2.HLen("hash_key")
	if hashLen != 2 {
		t.Errorf("hash_key length: got %d, want 2", hashLen)
	}

	setCard, _ := st2.SCard("set_key")
	if setCard != 2 {
		t.Errorf("set_key cardinality: got %d, want 2", setCard)
	}

	zsetCard, _ := st2.ZCard("zset_key")
	if zsetCard != 2 {
		t.Errorf("zset_key cardinality: got %d, want 2", zsetCard)
	}
}

func TestLoadNonExistentAOF(t *testing.T) {
	st := store.NewStore()
	executeCommand := func(values []resp.Value) resp.Value {
		return resp.Value{Type: resp.SimpleString, Str: "OK"}
	}

	err := LoadAOF("non_existent.aof", st, executeCommand)
	if err != nil {
		t.Errorf("Loading non-existent AOF should not error: %v", err)
	}
}
