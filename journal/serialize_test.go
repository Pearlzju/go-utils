package journal_test

import (
	"io"
	"io/ioutil"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/go-utils/journal"
	"github.com/Laisky/go-utils/journal/protocols"
)

func BenchmarkSerializer(b *testing.B) {
	fp, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		b.Fatalf("%+v", err)
	}
	defer fp.Close()
	defer os.Remove(fp.Name())
	b.Logf("create file name: %v", fp.Name())

	m := &protocols.Message{
		Tag: "test.sit",
		Id:  10,
		Msg: map[string]string{
			"log": "231289789471924",
		},
	}
	encoder := journal.NewDataEncoder(fp)
	b.Run("encoder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err = encoder.Write(m); err != nil {
				b.Fatalf("%+v", err)
			}
		}
	})

	encoder.Flush()
	fp.Sync()
	fp.Seek(0, 0)

	fs, _ := fp.Stat()
	b.Logf("file length: %v", fs.Size())

	decoder := journal.NewDataDecoder(fp)
	time.Sleep(1)
	v := &protocols.Message{}
	b.Run("decoder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err = decoder.Read(v); err == io.EOF {
				return
			} else if err != nil {
				return
				// b.Fatalf("%+v", err)
			}

			// b.Logf("got msg <%v>", i)
			if v.Tag != m.Tag ||
				v.Msg["log"] != m.GetMsg()["log"] {
				b.Fatal("load incorrect")
			}
		}
	})
}

func TestIdsSerializer(t *testing.T) {
	fp, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer fp.Close()
	defer os.Remove(fp.Name())
	t.Logf("create file name: %v", fp.Name())

	encoder := journal.NewIdsEncoder(fp)
	decoder := journal.NewIdsDecoder(fp)

	for id := int64(0); id < 1000; id++ {
		if err = encoder.Write(id); err != nil {
			t.Fatalf("%+v", err)
		}

		err = encoder.Write(math.MaxInt64 + id + 100)
		if err != nil {
			if !strings.Contains(err.Error(), "id should bigger than 0") {
				t.Fatalf("%+v", err)
			}
		}
	}

	if err = encoder.Flush(); err != nil {
		t.Fatalf("%+v", err)
	}

	fp.Seek(0, 0)
	ids, err := decoder.ReadAllToBmap()
	if err != nil {
		t.Fatalf("%+v", err)
	}

	t.Logf("got ids: %+v", ids)
	for id := 0; id < 2000; id++ {
		if id < 1000 && !ids.ContainsInt(id) {
			t.Fatalf("%v should in ids", id)
		}
		if id >= 1000 && ids.ContainsInt(id) {
			t.Fatalf("%v should not in ids", id)
		}
	}
}

func TestDataSerializer(t *testing.T) {
	fp, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer fp.Close()
	defer os.Remove(fp.Name())
	t.Logf("create file name: %v", fp.Name())

	encoder := journal.NewDataEncoder(fp)
	var (
		data = &protocols.Message{
			Tag: "test.sit",
			Id:  10,
			Msg: map[string]string{},
		}
	)

	var totalCnt = 10000
	for i := 0; i < totalCnt; i++ {
		data.Msg["log"] = "12345" + utils.RandomStringWithLength(200)
		if err = encoder.Write(data); err != nil {
			t.Fatalf("got error: %+v", err)
		}
	}

	encoder.Flush()
	fp.Sync()
	fp.Seek(0, 0)
	data = &protocols.Message{}
	decoder := journal.NewDataDecoder(fp)
	i := 0
	for {
		if err = decoder.Read(data); err == io.EOF {
			t.Log("all done")
			break
		} else if err != nil {
			t.Fatalf("got error: %+v", err)
		}

		i++
		if data.GetId() != 10 ||
			data.Tag != "test.sit" {
			t.Fatalf("msg error: %+v", data)
		}
	}
	if i != totalCnt {
		t.Fatalf("number not match")
	}
}
