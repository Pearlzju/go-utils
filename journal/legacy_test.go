package journal_test

import (
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/Laisky/go-utils/journal"
	"github.com/Laisky/go-utils/journal/protocols"
)

func TestLegacy(t *testing.T) {
	// create data files
	dataFp1, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer dataFp1.Close()
	defer os.Remove(dataFp1.Name())
	t.Logf("create file name: %v", dataFp1.Name())

	dataFp2, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer dataFp2.Close()
	defer os.Remove(dataFp2.Name())
	t.Logf("create file name: %v", dataFp2.Name())

	// create ids files
	idsFp1, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer idsFp1.Close()
	defer os.Remove(idsFp1.Name())
	t.Logf("create file name: %v", idsFp1.Name())

	idsFp2, err := ioutil.TempFile("", "journal-test")
	if err != nil {
		t.Fatalf("%+v", err)
	}
	defer idsFp2.Close()
	defer os.Remove(idsFp2.Name())
	t.Logf("create file name: %v", idsFp2.Name())

	// put data
	dataEncoder := journal.NewDataEncoder(dataFp1)
	if err = dataEncoder.Write(&protocols.Message{Tag: "test", Id: 1, Msg: map[string]string{"log": "111"}}); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = dataEncoder.Write(&protocols.Message{Tag: "test", Id: 2, Msg: map[string]string{"log": "222"}}); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = dataEncoder.Flush(); err != nil {
		t.Fatalf("got error: %+v", err)
	}

	dataEncoder = journal.NewDataEncoder(dataFp2)
	if err = dataEncoder.Write(&protocols.Message{Tag: "test", Id: 3, Msg: map[string]string{"log": "333"}}); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = dataEncoder.Write(&protocols.Message{Tag: "test", Id: 4, Msg: map[string]string{"log": "444"}}); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = dataEncoder.Flush(); err != nil {
		t.Fatalf("got error: %+v", err)
	}

	// put ids
	// except 2
	idsEncoder := journal.NewIdsEncoder(idsFp1)
	if err = idsEncoder.Write(1); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = idsEncoder.Write(2); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = idsEncoder.Flush(); err != nil {
		t.Fatalf("got error: %+v", err)
	}

	idsEncoder = journal.NewIdsEncoder(idsFp2)
	if err = idsEncoder.Write(3); err != nil {
		t.Fatalf("got error: %+v", err)
	}
	if err = idsEncoder.Flush(); err != nil {
		t.Fatalf("got error: %+v", err)
	}

	legacy := journal.NewLegacyLoader(
		[]string{dataFp1.Name(), dataFp2.Name()},
		[]string{idsFp1.Name(), idsFp2.Name()},
	)
	idmaps, err := legacy.LoadAllids()
	t.Logf("got ids: %+v", idmaps)
	// if err = idsEncoder.Write(22); err != nil {
	// 	t.Fatalf("got error: %+v", err)
	// }

	if !idmaps.ContainsInt(1) {
		t.Fatal("should contains 1")
	}
	if !idmaps.ContainsInt(2) {
		t.Fatal("should contains 2")
	}
	if !idmaps.ContainsInt(3) {
		t.Fatal("should contains 3")
	}
	if idmaps.ContainsInt(4) {
		t.Fatal("should not contains 4")
	}
	if idmaps.ContainsInt(5) {
		t.Fatal("should not contains 5")
	}

	dataIds := []int64{}
	for {
		data := &protocols.Message{}
		err = legacy.Load(data)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("got error: %+v", err)
		}
		dataIds = append(dataIds, data.GetId())
	}
	t.Logf("got dataIds: %+v", dataIds)
	for _, id := range dataIds {
		if id != 4 {
			t.Fatal("should equal to 4")
		}
	}
}

func TestEmptyLegacy(t *testing.T) {
	dir, err := ioutil.TempDir("", "journal-test")
	if err != nil {
		log.Fatal(err)
	}
	t.Logf("create directory: %v", dir)
	defer os.RemoveAll(dir)

	legacy := journal.NewLegacyLoader(
		[]string{},
		[]string{},
	)
	ids, err := legacy.LoadAllids()
	if err != nil {
		t.Fatalf("got error: %+v", err)
	}
	t.Logf("load ids: %+v", ids)

	data := &protocols.Message{}
	for {
		if err = legacy.Load(data); err == io.EOF {
			return
		} else if err != nil {
			t.Fatalf("got error: %+v", err)
		}
	}
}
