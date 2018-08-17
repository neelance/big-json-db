package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger"
)

var db *badger.DB
var txn *badger.Txn

func main() {
	var importOnly bool
	flag.BoolVar(&importOnly, "import-only", false, "")
	flag.Parse()

	jsonFile := flag.Arg(0)
	dbDir := jsonFile + ".db"

	_, err := os.Stat(dbDir)
	dbExists := err == nil

	opts := badger.DefaultOptions
	opts.Dir = dbDir
	opts.ValueDir = dbDir
	db, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if !dbExists {
		importJSON(jsonFile)
	}
	if importOnly {
		return
	}

	s := &http.Server{
		Addr: ":8080",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			query(r.Context(), w, strings.TrimSuffix(r.URL.Path, "/"))
		}),
	}
	fmt.Printf("Listening on %s", s.Addr)
	s.ListenAndServe()
}

func importJSON(jsonFile string) {
	f, err := os.Open(jsonFile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	txn = db.NewTransaction(true)
	dec := json.NewDecoder(&countingReader{r: f, size: fi.Size()})
	readValue(dec, make([]byte, 0, 1024))
	if err := txn.Commit(nil); err != nil {
		panic(err)
	}
}

type countingReader struct {
	r          io.Reader
	off        int64
	size       int64
	percentage int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.off += int64(n)

	newPercentage := r.off * 100 / r.size
	if newPercentage != r.percentage {
		r.percentage = newPercentage
		fmt.Printf("%d%%\n", r.percentage)
	}

	return n, err
}

func readValue(dec *json.Decoder, key []byte) {
	t, err := dec.Token()
	if err != nil {
		panic(err)
	}

	switch t := t.(type) {
	case json.Delim:
		switch t {
		case '{':
			for dec.More() {
				name, err := dec.Token()
				if err != nil {
					panic(err)
				}

				readValue(dec, append(key, []byte("/"+name.(string))...))
			}
			dec.Token() // }

		case '[':
			i := 0
			for dec.More() {
				readValue(dec, append(key, []byte("/"+strconv.Itoa(i))...))
				i++
			}
			dec.Token() // ]

		default:
			panic("bad delim")
		}

	default:
		data, err := json.Marshal(t)
		if err != nil {
			panic(err)
		}

		set(key, data)
	}
}

func set(key, data []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	if err := txn.Set(keyCopy, data); err != nil {
		if err != badger.ErrTxnTooBig {
			panic(err)
		}

		if err := txn.Commit(nil); err != nil {
			panic(err)
		}
		txn = db.NewTransaction(true)

		if err := txn.Set(keyCopy, data); err != nil {
			panic(err)
		}
	}
}

func query(ctx context.Context, w io.Writer, path string) {
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(path))
		if err == nil {
			v, err := item.Value()
			if err != nil {
				panic(err)
			}
			w.Write(v)
			w.Write([]byte("\n"))
			return nil
		}
		if err != badger.ErrKeyNotFound {
			panic(err)
		}

		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(path + "/")
		it.Seek(prefix)
		if !it.ValidForPrefix(prefix) {
			w.Write([]byte("null\n"))
			return nil
		}

		w.Write([]byte("{"))
		prefixLen := len(prefix)
		var currentKey [][]byte
		firstChild := true
		for it.ValidForPrefix(prefix) {
			if err := ctx.Err(); err != nil {
				panic(err)
			}

			item := it.Item()
			key := bytes.Split(item.Key()[prefixLen:], []byte("/"))

			for i, k := range currentKey {
				if len(key) < i || !bytes.Equal(key[i], k) {
					for len(currentKey) > i {
						w.Write([]byte(`}`))
						currentKey = currentKey[:len(currentKey)-1]
					}
					break
				}
			}

			for len(key)-1 > len(currentKey) {
				if !firstChild {
					w.Write([]byte(","))
				}
				name := key[len(currentKey)]
				w.Write([]byte(strconv.Quote(string(name))))
				w.Write([]byte(":{"))
				firstChild = true

				nameCopy := make([]byte, len(name))
				copy(nameCopy, name)
				currentKey = append(currentKey, nameCopy)
			}

			if !firstChild {
				w.Write([]byte(","))
			}
			name := key[len(key)-1]
			w.Write([]byte(strconv.Quote(string(name))))
			w.Write([]byte(":"))

			v, err := item.Value()
			if err != nil {
				panic(err)
			}
			w.Write(v)

			firstChild = false
			it.Next()
		}
		for range currentKey {
			w.Write([]byte(`}`))
		}
		w.Write([]byte("}\n"))
		return nil
	}); err != nil {
		panic(err)
	}
}
