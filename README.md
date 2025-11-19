# Redis Clone

A high-performance, Redis-compatible in-memory data store implementation written in Go. This project implements core Redis functionality including multiple data structures, persistence mechanisms, and memory management features.

## 🚀 Features

### Data Structures
- **Strings**: Basic key-value operations with atomic increment/decrement
- **Lists**: Double-ended list operations using quicklist implementation
- **Hashes**: Hash map operations for storing field-value pairs
- **Sets**: Unordered collections with set operations
- **Sorted Sets**: Ordered collections with score-based ranking using skip list

### Persistence
- **RDB (Redis Database)**: Point-in-time snapshots
  - SAVE (synchronous)
  - BGSAVE (background save)
- **AOF (Append-Only File)**: Command logging with configurable fsync policies
  - `always`: Sync after every write
  - `everysec`: Sync every second
  - `no`: Let OS handle syncing
  - BGREWRITEAOF for log compaction

### Memory Management
- Configurable memory limits with eviction policies:
  - `noeviction`: Return errors when memory limit is reached
  - `allkeys-lru`: Evict least recently used keys
  - `volatile-lru`: Evict LRU keys with expiration set
  - `allkeys-random`: Evict random keys
  - `volatile-random`: Evict random keys with expiration
  - `volatile-ttl`: Evict keys with shortest TTL

### Transactions
- MULTI/EXEC for atomic command execution
- WATCH/UNWATCH for optimistic locking
- DISCARD to abort transactions

### Protocol
- Full RESP (Redis Serialization Protocol) implementation
- Compatible with standard Redis clients (redis-cli, client libraries)

## 📋 Requirements

- Go 1.23.2 or higher
- Redis CLI (optional, for testing)

## 🛠️ Installation

### Build from Source

```bash
# Clone the repository
git clone https://github.com/lojhan/redis-clone.git
cd redis-clone

# Build the server
go build -o redis-server ./cmd/redis-server

# Run the server
./redis-server
```

## 🎯 Usage

### Starting the Server

Basic usage:
```bash
./redis-server
```

With custom configuration:
```bash
./redis-server \
  --port 6379 \
  --dbfilename dump.rdb \
  --appendonly true \
  --appendfilename appendonly.aof \
  --appendfsync everysec \
  --maxmemory 1073741824 \
  --maxmemory-policy allkeys-lru \
  --maxmemory-samples 5
```

### Configuration Options

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | 6379 | Port to listen on |
| `--dbfilename` | dump.rdb | RDB file name |
| `--appendonly` | false | Enable AOF persistence |
| `--appendfilename` | appendonly.aof | AOF file name |
| `--appendfsync` | everysec | AOF fsync policy (always/everysec/no) |
| `--maxmemory` | 0 | Maximum memory in bytes (0 = unlimited) |
| `--maxmemory-policy` | noeviction | Eviction policy |
| `--maxmemory-samples` | 5 | LRU sample size |

### Connecting with Redis CLI

```bash
redis-cli -p 6379
```

## 📚 Supported Commands

### Connection & Server
- `PING` - Test connection
- `ECHO` - Echo message
- `COMMAND` - Get command info
- `INFO` - Server information
- `CONFIG` - Get/set configuration
- `SHUTDOWN` - Shutdown server
- `DBSIZE` - Number of keys
- `FLUSHDB` / `FLUSHALL` - Clear database

### String Commands
- `SET key value [EX seconds] [PX milliseconds] [NX|XX]`
- `GET key`
- `DEL key [key ...]`
- `EXISTS key [key ...]`
- `TYPE key`
- `INCR key`
- `DECR key`

### List Commands
- `LPUSH key element [element ...]`
- `RPUSH key element [element ...]`
- `LPOP key`
- `RPOP key`
- `LLEN key`
- `LRANGE key start stop`

### Hash Commands
- `HSET key field value [field value ...]`
- `HGET key field`
- `HDEL key field [field ...]`
- `HEXISTS key field`
- `HLEN key`
- `HGETALL key`
- `HKEYS key`
- `HVALS key`

### Set Commands
- `SADD key member [member ...]`
- `SREM key member [member ...]`
- `SISMEMBER key member`
- `SMEMBERS key`
- `SCARD key`
- `SPOP key`

### Sorted Set Commands
- `ZADD key score member [score member ...]`
- `ZREM key member [member ...]`
- `ZSCORE key member`
- `ZCARD key`
- `ZRANK key member`
- `ZRANGE key start stop [WITHSCORES]`

### Persistence Commands
- `SAVE` - Synchronous save
- `BGSAVE` - Background save
- `LASTSAVE` - Last save timestamp
- `BGREWRITEAOF` - Rewrite AOF file

### Transaction Commands
- `MULTI` - Start transaction
- `EXEC` - Execute transaction
- `DISCARD` - Discard transaction
- `WATCH key [key ...]` - Watch keys
- `UNWATCH` - Unwatch all keys

## 🏗️ Architecture

### Core Components

```
├── cmd/redis-server/     # Server entry point
├── internal/
│   ├── command/          # Command implementations
│   ├── persistence/      # RDB and AOF handlers
│   ├── resp/            # RESP protocol parser/serializer
│   ├── server/          # TCP server and client handling
│   └── store/           # Data structures and storage
│       ├── hashtable.go  # Hash table implementation
│       ├── quicklist.go  # List implementation
│       ├── set.go       # Set implementation
│       ├── skiplist.go  # Sorted set implementation
│       ├── eviction.go  # Memory eviction policies
│       └── store.go     # Main storage interface
```

### Design Principles

1. **Single-threaded Event Loop**: Eliminates synchronization overhead and provides inherent atomicity
2. **Non-blocking I/O**: Uses multiplexing to handle thousands of concurrent connections
3. **Memory Efficiency**: Optimized data structures with LRU tracking
4. **Persistence Options**: Flexible RDB snapshots and AOF logging
5. **RESP Protocol**: Full compatibility with Redis clients

## 🧪 Testing

### Run Unit Tests

```bash
go test ./... -v
```

## 📊 Performance Characteristics

- **Atomicity**: All commands are atomic due to single-threaded execution
- **Concurrency**: Handles 10,000+ concurrent connections via I/O multiplexing
- **Memory**: Configurable limits with multiple eviction strategies
- **Persistence**: Configurable durability vs performance trade-offs

## 🔍 Implementation Details

### Data Structure Encodings

- **Strings**: Int encoding for numeric values, embstr for short strings, raw for large strings
- **Lists**: Quicklist (combination of ziplist nodes) for space efficiency
- **Hashes**: Hash table with SipHash for collision resistance
- **Sets**: Hash table or intset for integer-only sets
- **Sorted Sets**: Skip list with hash table for O(log n) operations

### Memory Eviction

The LRU implementation uses:
- 24-bit LRU clock for timestamp tracking
- Approximate LRU via sampling (configurable samples)
- Support for both global and volatile key eviction

### Persistence

**RDB Format**: Binary snapshot format compatible with Redis
- Magic header: `REDIS0009`
- Type-value pairs with length encoding
- CRC64 checksum for integrity

**AOF Format**: Text-based command log
- Each command stored in RESP format
- Background rewriting for compaction
- Configurable fsync policies

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## 📝 License

This project is for educational purposes. Please check the license before using in production.

## 🎓 Learning Resources

This implementation is based on the Redis technical specification and demonstrates:
- Event-driven architecture
- Non-blocking I/O patterns
- Data structure design
- Persistence strategies
- Memory management techniques

## 🔗 References

- [Redis Documentation](https://redis.io/documentation)
- [RESP Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
