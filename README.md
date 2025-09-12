[![progress-banner](https://backend.codecrafters.io/progress/redis/ea988461-72a1-4139-8fa7-7333bfcdb254)](https://app.codecrafters.io/users/Henrik-13?r=2qF)

# Redis Clone in Python

A Redis-compatible server implementation in Python, built as part of the [CodeCrafters "Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

## Features

This Redis clone supports:

### Core Commands
- `PING` - Test server connectivity
- `ECHO` - Echo back a message
- `SET` / `GET` - String operations with optional expiration
- `INCR` - Increment integer values

### Data Structures
- **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `LRANGE`, `LLEN`, `BLPOP`
- **Streams**: `XADD`, `XRANGE`, `XREAD` with blocking support
- **Sorted Sets**: `ZADD`, `ZRANK`, `ZRANGE`, `ZCARD`, `ZSCORE`, `ZREM`
- **Geospatial**: `GEOADD`, `GEOPOS`, `GEODIST`, `GEOSEARCH`

### Advanced Features
- **Replication**: Master-replica setup with `PSYNC`, `REPLCONF`, `WAIT`
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`
- **Pub/Sub**: `SUBSCRIBE`, `PUBLISH`
- **Persistence**: RDB file loading
- **Configuration**: `CONFIG GET`

## Project Structure

```
app/
├── main.py                # Entry point
├── server.py              # Main server class
├── parsers/               # Protocol parsers
│   ├── __init__.py
│   ├── command_parser.py  # RESP protocol parser
│   └── rdb_parser.py      # RDB file parser
├── stores/                # Data storage implementations
│   ├── __init__.py
│   ├── string_store.py
│   ├── list_store.py
│   ├── stream_store.py
│   └── sorted_set_store.py
└── utils/                 # Utility modules
    ├── __init__.py
    └── geohash.py         # Geospatial encoding
```

## Installation & Usage

### Prerequisites
- Python 3.8 or higher
- pipenv (for dependency management)

### Running the Server

```bash
# Install dependencies
pipenv install

# Start the server on default port (6379)
./your_program.sh

# Start with custom configuration
./your_program.sh --port 6380 --dir ./data --dbfilename dump.rdb

# Start as replica
./your_program.sh --replicaof "localhost 6379"
```

### Command Line Options

- `--port`: Server port (default: 6379)
- `--replicaof`: Master server for replication ("host port")
- `--dir`: Directory for persistence files
- `--dbfilename`: RDB filename for persistence

## Development

### Testing with Redis CLI

```bash
# Connect to your server
redis-cli -p 6379

# Test basic operations
> PING
PONG
> SET mykey "Hello World"
OK
> GET mykey
"Hello World"
```

### Architecture

The server uses:
- **Thread-per-connection model** for handling multiple clients
- **RESP (Redis Serialization Protocol)** for client communication
- **Thread-safe stores** with proper locking mechanisms
- **Event-driven blocking operations** for commands like `BLPOP` and `XREAD`

## Contributing

This is an educational project. Feel free to:
- Add new Redis commands
- Improve performance
- Add tests
- Enhance documentation

## License

This project is part of the CodeCrafters challenge and is for educational purposes.

## Acknowledgments

- [CodeCrafters](https://codecrafters.io) for the excellent challenge
- Redis team for the amazing database and protocol design