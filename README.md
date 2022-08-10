# bitcask-lite

A database and server — partial implementation of the Bitcask paper: https://riak.com/assets/bitcask-intro.pdf

- low latency per item read or written
- handles datasets larger than RAM
- human readable data format
- small specification
- just uses the Go standard library

## Spec

Items are stored in log files on disk to persist data. Keys are kept in-memory and point to values in log files. All new items are written to the active log file.

Log files contain any number of adjacent items with the schema: `timestamp, keySize, valueSize, key, value,`.

An item with a key of `a` and a value of `b` that expires on 10 Aug 2022 looks like this on disk:

```text
1759300313415,1,1,a,b,
```

In memory, it's something like this:

```golang
&Item {
	"1660073049777-XVlBzgbaiCMRAjWw" // log file name (timestamp + random)
	1759300313415 // expire
	20 // value position
	1 // value size
}
```

Not yet implemented: log file merging, hint files.

### HTTP API

- GET `/get?key=a`
- POST `/set?key=b&expire=1759300313415`
  - Body is read as the value
  - `expire` is optional (default is infinite)
- DELETE `/delete?key=c

## Performance

The in-memory store is a concurrent map of maps. Each map shard has a lock to allow concurrent access.

Reading a value requires a single disk seek.

Only one goroutine may write to the the active log file at a time so read-heavy workloads are ideal.

## Tests

Tests perform real I/O to disk and generate new files every run.

```bash
pip install -r requirements.txt
python e2e.py # run e2e tests covering the main function
go test ./... # unit tests
```

## Deployment

It's a fairly standard Go application. Set `PORT`, `DATABASE_DIR`, and run.

Deploys to `railway.app` with zero configuration (presumably most platforms as a service).
