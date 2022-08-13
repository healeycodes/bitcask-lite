# bitcask-lite

A key/value database and server. Partial implementation of the Bitcask paper: https://riak.com/assets/bitcask-intro.pdf

- Low latency per item read or written
- Handles datasets larger than RAM
- Human readable data format
- Small specification
- Human-readable data format
- Just uses the Go standard library

## Spec

Keys are kept in-memory and point to values in log files. Log files are append-only and contain any number of adjacent items with the schema: `timestamp, keySize, valueSize, key, value,`.

An item with a key of `a` and a value of `b` that expires on 10 Aug 2022 looks like this in a log file:

```text
1759300313415,1,1,a,b,
```

Not yet implemented: checksums, log file merging, hintfiles.

### HTTP API

- GET: `/get?key=a`
- POST: `/set?key=b&expire=1759300313415`
  - HTTP body is read as the value
  - `expire` is optional (default is infinite)
- DELETE: `/delete?key=c`

## Performance

The key store is a concurrent map with locking map shards.

Reading a value requires a single disk seek.

Only one goroutine may write to the the active log file at a time so read-heavy workloads are ideal.

## Tests

Tests perform real I/O to disk and generate new files every run.

```bash
pip install -r requirements.txt # (it just uses the requests library)
python e2e.py # run e2e tests covering the main function
go test ./... # unit tests
```

## Deployment

As this is fairly standard Go application: set `PORT`, `DATABASE_DIR`, and run.

It deploys to `railway.app` with zero configuration (presumably most platforms-as-a-service as well).
