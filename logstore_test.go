package main

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"
)

// Use fixtures to test getting values from a loaded log file
func TestGet(t *testing.T) {
	dir, err := filepath.Abs(path.Join("test_dbs", "basic"))
	if err != nil {
		panic(err)
	}

	logStore, err := CreateLogStore(dir, nil)
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	k := "a"
	v := "b"
	found, err := logStore.StreamGet(k, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k, err)
	}
	if b.String() != v {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v)
	}

	b.Reset()

	k = "c"
	v = "dd"
	found, err = logStore.StreamGet(k, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k, err)
	}
	if b.String() != v {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v)
	}

	b.Reset()

	k = "e"
	v = "f"
	found, err = logStore.StreamGet(k, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k, err)
	}
	if b.String() != v {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v)
	}
}

// Use fixtures to test (not) getting expired values from a loaded log file
func TestGetExpired(t *testing.T) {
	dir, err := filepath.Abs(path.Join("test_dbs", "basic"))
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := CreateLogStore(dir, nil)
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	k := "x"
	found, err := logStore.StreamGet(k, w)
	if found != false && err != nil {
		t.Errorf("for \"%s\", found should be false and err should be nil: %s", k, err)
	}
	if len(b.String()) != 0 {
		t.Errorf("for \"%s\", bytes shouldn't be written", k)
	}
}

// Use fixtures to test (not) getting overwritten/deleted values from a loaded log file
func TestGetExpiredLogFileOverwrite(t *testing.T) {
	dir, err := filepath.Abs(path.Join("test_dbs", "basic"))
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := CreateLogStore(dir, nil)
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	k := "q"
	found, err := logStore.StreamGet(k, w)
	if found != false && err != nil {
		t.Errorf("for \"%s\", found should be false and err should be nil: %s", k, err)
	}
	if len(b.String()) != 0 {
		t.Errorf("for \"%s\", bytes shouldn't be written", k)
	}
}

// Test setting/getting in an active log file
func TestSet(t *testing.T) {
	tempDir, err := ioutil.TempDir("test_dbs/temp", "testset")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dir, err := filepath.Abs(tempDir)
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := CreateLogStore(dir, nil)
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	// Add item
	k := "n"
	v := "m"
	err = logStore.Set(k, int(time.Now().UnixMilli())+1000, []byte(v))
	if err != nil {
		t.Errorf("couldn't set key \"%s\" %s", k, err)
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	// Check add was successful
	found, err := logStore.StreamGet(k, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k, err)
	}
	if b.String() != v {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v)
	}

	b.Reset()

	// Overwrite existing item
	k = "n"
	v = "mm"
	err = logStore.Set(k, int(time.Now().UnixMilli())+1000, []byte(v))
	if err != nil {
		t.Errorf("couldn't set key \"%s\" for the second time %s", k, err)
	}

	// Check overwrite was successful
	found, err = logStore.StreamGet(k, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k, err)
	}
	if b.String() != v {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v)
	}

	b.Reset()

	// Add already expired item
	k = "ex"
	v = "1"
	err = logStore.Set(k, int(time.Now().UnixMilli())-1000, []byte("1"))
	if err != nil {
		t.Errorf("couldn't set key \"%s\" %s", k, err)
	}

	// Check expired item isn't found
	found, err = logStore.StreamGet(k, w)
	if found != false && err != nil {
		t.Errorf("for \"%s\", found should be false and err should be nil: %s", k, err)
	}
	if len(b.String()) != 0 {
		t.Errorf("not found items should write to writer")
	}
}

// Test log files are rolled correctly
func TestRollingLogFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("test_dbs/temp", "testrollinglogfile")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dir, err := filepath.Abs(tempDir)
	if err != nil {
		log.Fatal(err)
	}

	// Use a small max bytes to check rolling the log file works correctly
	logStore, err := CreateLogStore(dir, &LogStoreOptions{maxLogFileBytes: 32})
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	// Add to the first log file
	k1 := "a"
	v1 := "________________1"
	err = logStore.Set(k1, int(time.Now().UnixMilli())+1000, []byte(v1))
	if err != nil {
		t.Errorf("couldn't set key \"%s\" %s", k1, err)
	}

	// Add to the second log file
	k2 := "b"
	v2 := "________________2"
	err = logStore.Set(k2, int(time.Now().UnixMilli())+1000, []byte(v2))
	if err != nil {
		t.Errorf("couldn't set key \"%s\" %s", k2, err)
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	// Check adds were successful
	found, err := logStore.StreamGet(k1, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k1, err)
	}
	if b.String() != v1 {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v1)
	}

	b.Reset()

	found, err = logStore.StreamGet(k2, w)
	if found != true && err != nil {
		t.Errorf("for \"%s\", found should be true and err should be nil: %s", k2, err)
	}
	if b.String() != v2 {
		t.Errorf("got \"%s\"; want \"%s\"", b.String(), v2)
	}
}

// Test writing and loading a log file for correctness
func TestLoadLogFile(t *testing.T) {
	tempDir, err := ioutil.TempDir("test_dbs/temp", "testloadlogfile")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dir, err := filepath.Abs(tempDir)
	if err != nil {
		log.Fatal(err)
	}

	logStore, err := CreateLogStore(dir, nil)
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	// Cover edge case of empty values
	k := []byte("a")
	keys = append(keys, k)
	v := []byte("")
	values = append(values, v)
	logStore.Set(string(k), int(time.Now().UnixMilli())+100000, v)

	// Empty keys too
	k = []byte("")
	keys = append(keys, k)
	v = []byte("zz")
	values = append(values, v)
	logStore.Set(string(k), int(time.Now().UnixMilli())+100000, v)

	// Add some random items
	for i := 0; i < 8; i++ {
		k := rndFileString(16)
		keys = append(keys, k)

		v := rndData(256)
		values = append(values, v)
		logStore.Set(string(k), int(time.Now().UnixMilli())+100000, v)
	}

	logStore2, err := CreateLogStore(dir, nil)
	if err != nil {
		t.Errorf("couldn't create log store %s", err)
		return
	}

	for i := range keys {
		k := keys[i]
		v := values[i]
		var b bytes.Buffer
		w := bufio.NewWriter(&b)

		found, err := logStore2.StreamGet(string(k), w)
		if found != true && err != nil {
			t.Errorf("for \"%s\", found should be true and err should be nil: %s", k, err)
		}
		if !bytes.Equal(b.Bytes(), v) {
			t.Errorf("got \"%s\"; want \"%s\"", b.String(), v)
		}
	}
}

func rndData(length int) []byte {
	data := make([]byte, length)
	rand.Read(data)
	return data
}
