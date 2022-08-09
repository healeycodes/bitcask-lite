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

	found, err := logStore.StreamGet("a", w)
	if found != true && err != nil {
		t.Errorf("for \"a\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "b" {
		t.Errorf("got \"%s\"; want \"b\"", b.String())
	}

	b.Reset()

	found, err = logStore.StreamGet("c", w)
	if found != true && err != nil {
		t.Errorf("for \"c\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "dd" {
		t.Errorf("got \"%s\"; want \"dd\"", b.String())
	}

	b.Reset()

	found, err = logStore.StreamGet("e", w)
	if found != true && err != nil {
		t.Errorf("for \"e\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "f" {
		t.Errorf("got \"%s\"; want \"f\"", b.String())
	}
}

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

	found, err := logStore.StreamGet("x", w)
	if found != false && err != nil {
		t.Errorf("for \"x\", found should be false and err should be nil: %s", err)
	}
	if len(b.String()) != 0 {
		t.Errorf("for \"x\", bytes shouldn't be written")
	}
}

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
	err = logStore.Set("n", int(time.Now().UnixMilli())+1000, []byte("m"))
	if err != nil {
		t.Errorf("couldn't set key \"n\" %s", err)
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	// Check add was successful
	found, err := logStore.StreamGet("n", w)
	if found != true && err != nil {
		t.Errorf("for \"n\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "m" {
		t.Errorf("got \"%s\"; want \"m\"", b.String())
	}

	b.Reset()

	// Overwrite existing item
	err = logStore.Set("n", int(time.Now().UnixMilli())+1000, []byte("mm"))
	if err != nil {
		t.Errorf("couldn't set key \"n\" for the second time %s", err)
	}

	// Check overwrite was successful
	found, err = logStore.StreamGet("n", w)
	if found != true && err != nil {
		t.Errorf("for \"n\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "mm" {
		t.Errorf("got \"%s\"; want \"mm\"", b.String())
	}

	b.Reset()

	// Add already expired item
	err = logStore.Set("ex", int(time.Now().UnixMilli())-1000, []byte("1"))
	if err != nil {
		t.Errorf("couldn't set key \"ex\" %s", err)
	}

	// Check expired item isn't found
	found, err = logStore.StreamGet("ex", w)
	if found != false && err != nil {
		t.Errorf("for \"ex\", found should be false and err should be nil: %s", err)
	}
	if len(b.String()) != 0 {
		t.Errorf("not found items should write to writer")
	}
}

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
	err = logStore.Set("a", int(time.Now().UnixMilli())+1000, []byte("________________1"))
	if err != nil {
		t.Errorf("couldn't set key \"n\" %s", err)
	}

	// Add to the second log file
	err = logStore.Set("b", int(time.Now().UnixMilli())+1000, []byte("________________2"))
	if err != nil {
		t.Errorf("couldn't set key \"n\" %s", err)
	}

	var b bytes.Buffer
	w := bufio.NewWriter(&b)

	// Check adds were successful
	found, err := logStore.StreamGet("a", w)
	if found != true && err != nil {
		t.Errorf("for \"a\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "________________1" {
		t.Errorf("got \"%s\"; want \"________________1\"", b.String())
	}

	b.Reset()

	found, err = logStore.StreamGet("b", w)
	if found != true && err != nil {
		t.Errorf("for \"b\", found should be true and err should be nil: %s", err)
	}
	if b.String() != "________________2" {
		t.Errorf("got \"%s\"; want \"________________2\"", b.String())
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
