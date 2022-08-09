package main

import (
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
)

func main() {
	logDir := getEnv("DATABASE_DIR", DEFAULT_DATABASE_DIR)
	logStore, err := CreateLogStore(logDir, nil)
	if err != nil {
		log.Fatalf("couldn't create log store: %s", err)
	}

	http.HandleFunc("/get", get(logStore))
	http.HandleFunc("/set", set(logStore))
	http.ListenAndServe(":"+getEnv("PORT", "8000"), nil)
}

func get(logStore *LogStore) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(400)
			w.Write([]byte("missing ?key"))
			return
		}

		found, err := logStore.StreamGet(key, w)
		if err != nil {
			log.Printf("couldn't get %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
		if !found {
			w.WriteHeader(404)
			return
		}
		if err != nil {
			log.Printf("couldn't get %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
	}
}

func set(logStore *LogStore) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, req *http.Request) {
		key := req.URL.Query().Get("key")
		if key == "" {
			w.WriteHeader(400)
			w.Write([]byte("missing ?key"))
			return
		}

		var expire int
		_expire := req.URL.Query().Get("expire")
		if _expire == "" {
			expire = math.MaxInt64 // Unix milliseconds
		} else {
			i, err := strconv.Atoi(_expire)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte("?expire must be an integer"))
				return
			}
			expire = i
		}

		value, err := io.ReadAll(req.Body)
		if err != nil {
			log.Printf("couldn't set %s: %s", key, err)
			w.WriteHeader(500)
			return
		}

		err = logStore.Set(key, expire, value)
		if err != nil {
			log.Printf("couldn't set %s: %s", key, err)
			w.WriteHeader(500)
			return
		}
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
