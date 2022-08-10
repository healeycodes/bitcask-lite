package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

const DEFAULT_DATABASE_DIR = "./store"
const MAX_LOG_FILE_BYTES = 1024 * 1024 * 32
const SHARDS = 128

type LogStore struct {
	logDir    string
	keys      *MapOfMaps[Item]
	logFile   *os.File
	logFileMu sync.Mutex
	opts      *LogStoreOptions
}

type LogStoreOptions struct {
	maxLogFileBytes int
}

// StreamGet streams a value from the log store.
// Goroutine safe due to map sharps with locks
func (logStore *LogStore) StreamGet(key string, w io.Writer) (bool, error) {
	access := logStore.keys.AccessShard(key)
	defer access.Unlock()
	item, found := logStore.keys.Get(key)

	if !found {
		return false, nil
	} else if int(time.Now().UnixMilli()) >= item.expire {
		// Clean up expired items
		logStore.keys.Delete(key)
		return false, nil
	}

	f, err := os.Open(item.file)
	if err != nil {
		return false, fmt.Errorf("couldn't open log file %s: %s", logStore.logFile.Name(), err)
	}
	defer f.Close()

	_, err = f.Seek(int64(item.valuePos), 0)
	if err != nil {
		return false, fmt.Errorf("couldn't seek in %s: %s", logStore.logFile.Name(), err)
	}

	_, err = io.CopyN(w, f, int64(item.valueSize))
	if err != nil {
		return true, err
	}
	return true, nil
}

// Set sets a value. Setting `expire` to 0 is effectively a delete operation.
// Goroutine safe due to map sharps with locks, and a log file lock
func (logStore *LogStore) Set(key string, expire int, value []byte) error {
	access := logStore.keys.AccessShard(key)
	defer access.Unlock()
	logStore.logFileMu.Lock()
	defer logStore.logFileMu.Unlock()

	fi, err := logStore.logFile.Stat()
	if err != nil {
		return fmt.Errorf("couldn't stat log file %s: %s", logStore.logFile.Name(), err)
	}
	end := int(fi.Size())

	line := []byte(fmt.Sprintf("%d,%d,%d,%s,", expire, len(key), len(value), key))
	lineLength := len(line) + len(value) + 1 // And the ending comma

	// Roll log file if we need to
	if end+lineLength >= logStore.opts.maxLogFileBytes {
		err = logStore.nextLogFile()
		if err != nil {
			return err
		}

		// New log files are empty
		end = 0
	}

	data := append(append(line, value...), ","...)
	_, err = logStore.logFile.Write(data)
	if err != nil {
		return fmt.Errorf("couldn't write to %s: %s", logStore.logFile.Name(), err)
	}

	item := Item{
		logStore.logFile.Name(),
		expire,
		end + len(line),
		len(value),
	}

	// To support deletes, instead of adding expired items
	// to the in-memory key dictionary, remove the old key
	if int(time.Now().UnixMilli()) >= expire {
		logStore.keys.Delete(string(key))
		return nil
	} else {
		logStore.keys.Set(key, item)
	}

	return nil
}

func (logStore *LogStore) nextLogFile() error {
	defer logStore.logFile.Close()
	logFile, err := createLogFile(logStore.logDir)
	if err != nil {
		return err
	}
	logStore.logFile = logFile
	return nil
}

type Item struct {
	file      string
	expire    int
	valuePos  int
	valueSize int
}

func parseLogFile(path string) (map[string]Item, error) {
	const COMMA byte = 44

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("couldn't open log file %s: %s", path, err)
	}
	defer f.Close()

	keys := make(map[string]Item)

	r := bufio.NewReader(f)
	cur := 0
	for {
		_expire, err := r.ReadBytes(COMMA)
		cur += len(_expire)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("couldn't parse expire %s", err)
		}
		expire, err := strconv.Atoi(string(_expire[:len(_expire)-1]))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse expire %s", err)
		}

		_keySize, err := r.ReadBytes(COMMA)
		cur += len(_keySize)
		if err != nil {
			return nil, fmt.Errorf("couldn't skip keySize: %s", err)
		}
		keySize, err := strconv.Atoi(string(_keySize[:len(_keySize)-1]))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse keySize: %s", err)
		}

		_valueSize, err := r.ReadBytes(COMMA)
		cur += len(_valueSize)
		if err != nil {
			return nil, fmt.Errorf("couldn't parse valueSize: %s", err)
		}
		valueSize, err := strconv.Atoi(string(_valueSize[:len(_valueSize)-1]))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse valueSize: %s", err)
		}

		key := make([]byte, keySize+1) // Read key (+ 1 for the comma between metadata)
		n, err := r.Read(key)
		cur += n
		if err != nil {
			return nil, fmt.Errorf("couldn't parse key: %s", err)
		}
		key = key[:len(key)-1]

		valueOffset := cur // The value can be found at the current cursor

		n, err = r.Discard(valueSize + 1) // Skip value (+ 1 for the comma between metadata)
		cur += n
		if err != nil {
			return nil, fmt.Errorf("during key (%s) couldn't skip value: %s", key, err)
		}

		if int(time.Now().UnixMilli()) < expire {
			keys[string(key)] = Item{
				path,
				expire,
				valueOffset,
				valueSize,
			}
		} else {
			// Don't load expired items into memory, clean up items that have been overwritten
			delete(keys, string(key))
		}
	}
	return keys, nil
}

// CreateLogStore creates a new log store and loads existing log files from disk
func CreateLogStore(logDir string, opts *LogStoreOptions) (*LogStore, error) {
	if opts == nil {
		opts = &LogStoreOptions{maxLogFileBytes: MAX_LOG_FILE_BYTES}
	}

	err := os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("couldn't create directory %s: %s", logDir, err)
	}

	// Load log files from disk one-by-one in lexical order (file names
	// start with a timestamp). No need to do this concurrently yet
	logFiles, err := ioutil.ReadDir(logDir)
	if err != nil {
		return nil, fmt.Errorf("couldn't read directory %s: %s", logDir, err)
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].Name() < logFiles[j].Name()
	})

	mapOfMaps := NewMapOfMaps[Item](SHARDS)
	for _, fileInfo := range logFiles {
		keys, err := parseLogFile(path.Join(logDir, fileInfo.Name()))
		if err != nil {
			return nil, fmt.Errorf("couldn't parse log file %s: %s", path.Join(logDir, fileInfo.Name()), err)
		}
		mapOfMaps.MSet(keys)
	}

	var logFile *os.File
	if len(logFiles) > 0 {
		latest := logFiles[len(logFiles)-1]
		latestPath := path.Join(logDir, latest.Name())
		fi, err := os.Stat(latestPath)
		if err != nil {
			return nil, fmt.Errorf("couldn't stat log file %s: %s", path.Join(logDir, latestPath), err)
		}
		if fi.Size() >= int64(opts.maxLogFileBytes) {
			// If the latest log file on disk is at capacity create a new one
			logFile, err = createLogFile(logDir)
			if err != nil {
				return nil, err
			}
		} else {
			// Otherwise, open the latest log file because there's still room
			logFile, err = os.OpenFile(latestPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return nil, err
			}
		}
	} else {
		// If this is a new directory, create the first log file
		logFile, err = createLogFile(logDir)
		if err != nil {
			return nil, err
		}
	}

	return &LogStore{
		logDir,
		mapOfMaps,
		logFile,
		sync.Mutex{},
		opts,
	}, nil
}

func createLogFile(logDir string) (*os.File, error) {
	id := fmt.Sprintf("%d-%s", time.Now().UnixMilli(), rndFileString(16))
	logFile, err := os.Create(path.Join(logDir, id))
	if err != nil {
		return nil, fmt.Errorf("couldn't create log file %s: %s", path.Join(logDir, id), err)
	}
	return logFile, nil
}

func rndFileString(length int) []byte {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}
