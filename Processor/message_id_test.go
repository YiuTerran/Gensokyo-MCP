package Processor

import (
	"encoding/binary"
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hoshinonyaruko/gensokyo-mcp/wsclient"
	bolt "go.etcd.io/bbolt"
)

func seedTestMessageIDState(t *testing.T, path string, next uint32) {
	t.Helper()
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("protocol_ids"))
		if err != nil {
			return err
		}
		var raw [4]byte
		binary.BigEndian.PutUint32(raw[:], next)
		return bucket.Put([]byte("next_message_id"), raw[:])
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
}

func openTestMessageIDAllocator(t *testing.T, path string) {
	t.Helper()
	if err := wsclient.CloseMessageIDAllocator(); err != nil {
		t.Fatal(err)
	}
	if err := wsclient.OpenMessageIDAllocator(path); err != nil {
		t.Fatal(err)
	}
}

func TestSyntheticMessageIDsPersistAndStayWithinInt32Bounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "message_ids.db")
	seedTestMessageIDState(t, path, 1234)
	openTestMessageIDAllocator(t, path)

	first, err := NextSyntheticMessageID()
	if err != nil {
		t.Fatal(err)
	}
	if first != 1235 || first <= 0 || int64(first) > math.MaxInt32 {
		t.Fatalf("first synthetic ID outside expected positive int32 sequence: %d", first)
	}
	if err := wsclient.CloseMessageIDAllocator(); err != nil {
		t.Fatal(err)
	}

	openTestMessageIDAllocator(t, path)
	second, err := NextSyntheticMessageID()
	if err != nil {
		t.Fatal(err)
	}
	if second != first+1 || second <= 0 || int64(second) > math.MaxInt32 {
		t.Fatalf("synthetic ID did not persist across allocator restart: first=%d second=%d", first, second)
	}
	if err := wsclient.CloseMessageIDAllocator(); err != nil {
		t.Fatal(err)
	}

	seedTestMessageIDState(t, path, math.MaxInt32-1)
	openTestMessageIDAllocator(t, path)
	last, err := NextSyntheticMessageID()
	if err != nil {
		t.Fatal(err)
	}
	if last != math.MaxInt32 {
		t.Fatalf("allocator did not reach the positive int32 boundary: %d", last)
	}
	if _, err := NextSyntheticMessageID(); err == nil {
		t.Fatal("allocator wrapped or exceeded int32 after exhaustion")
	}
	if err := wsclient.CloseMessageIDAllocator(); err != nil {
		t.Fatal(err)
	}
}

func TestSyntheticMessageIDAllocationRequiresPersistentAllocator(t *testing.T) {
	if err := wsclient.CloseMessageIDAllocator(); err != nil {
		t.Fatal(err)
	}
	if _, err := NextSyntheticMessageID(); err == nil {
		t.Fatal("synthetic ID allocation succeeded without an open persistent allocator")
	}
}

func TestSyntheticMessageIDsConcurrent(t *testing.T) {
	const count = 256
	path := filepath.Join(t.TempDir(), "message_ids.db")
	seedTestMessageIDState(t, path, 9000)
	openTestMessageIDAllocator(t, path)
	defer wsclient.CloseMessageIDAllocator()

	type result struct {
		id  int32
		err error
	}
	ids := make(chan result, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := NextSyntheticMessageID()
			ids <- result{id: id, err: err}
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[int32]bool, count)
	for item := range ids {
		if item.err != nil {
			t.Fatal(item.err)
		}
		if item.id <= 0 || int64(item.id) > math.MaxInt32 {
			t.Fatalf("concurrent synthetic ID outside positive int32: %d", item.id)
		}
		if seen[item.id] {
			t.Fatalf("duplicate synthetic message ID %d", item.id)
		}
		seen[item.id] = true
	}
}
