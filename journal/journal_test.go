package journal_test

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/go-utils/journal"
	"github.com/Laisky/go-utils/journal/protocols"
)

func BenchmarkLock(b *testing.B) {
	b.Run("mutex", func(b *testing.B) {
		l := &sync.Mutex{}
		for i := 0; i < b.N; i++ {
			l.Lock()
			l.Unlock()
		}
	})

	b.Run("atomic", func(b *testing.B) {
		var (
			i uint64 = 0
		)
		for j := 0; j < b.N; j++ {
			atomic.CompareAndSwapUint64(&i, 0, 1)
			atomic.CompareAndSwapUint64(&i, 1, 0)
		}
	})
}

func fakedata(length int) map[int64]interface{} {
	m := make(map[int64]interface{}, length)
	for i := 0; i < length; i++ {
		m[int64(i)] = utils.RandomStringWithLength(100 + i)
	}

	return m
}

func TestJournal(t *testing.T) {
	dir, err := ioutil.TempDir("", "journal-test")
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("create directory: %v", dir)
	defer os.RemoveAll(dir)

	cfg := &journal.JournalConfig{
		BufDirPath:             dir,
		BufSizeBytes:           100,
		RotateCheckIntervalNum: 50,
	}
	j := journal.NewJournal(cfg)
	data := &protocols.Message{
		Tag: "test",
	}
	threshold := int64(50)

	for id, _ := range fakedata(1000) {
		data.Id = id
		if err = j.WriteData(data); err != nil {
			t.Fatalf("got error: %+v", err)
		}

		if id < threshold {
			continue
		}

		if err = j.WriteId(id); err != nil {
			t.Fatalf("got error: %+v", err)
		}
	}

	if err = j.Rotate(); err != nil {
		t.Fatalf("got error: %+v", err)
	}

	if !j.LockLegacy() {
		t.Fatal("can not lock legacy")
	}
	i := 0
	for {
		if err = j.LoadLegacyBuf(data); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("got error: %+v", err)
		}

		t.Logf("got: %v", data.GetId())
		if data.GetId() >= threshold {
			t.Errorf("should not got id: %+v", data.GetId())
		}

		i++
	}

	if i != int(threshold) {
		t.Fatalf("expect %v, got %v", threshold, i)
	}

}

func BenchmarkJournal(b *testing.B) {
	dir, err := ioutil.TempDir("", "journal-test")
	if err != nil {
		log.Fatal(err)
	}
	b.Logf("create directory: %v", dir)
	defer os.RemoveAll(dir)

	cfg := &journal.JournalConfig{
		BufDirPath:   dir,
		BufSizeBytes: 100,
	}
	j := journal.NewJournal(cfg)
	data := &protocols.Message{
		Id:  1,
		Tag: "test",
		Msg: map[string]string{"log": "xxx"},
	}
	id := int64(1)

	b.Run("store", func(b *testing.B) {

		if err = j.WriteData(data); err != nil {
			b.Fatalf("got error: %+v", err)
		}

		if err = j.WriteId(id); err != nil {
			b.Fatalf("got error: %+v", err)
		}
	})

	if err = j.Rotate(); err != nil {
		b.Fatalf("got error: %+v", err)
	}

	b.Run("load", func(b *testing.B) {
		if err = j.LoadLegacyBuf(data); err == io.EOF {
			return
		} else if err != nil {
			b.Fatalf("got error: %+v", err)
		}
	})
}
