package indexer

import (
	"bytes"
	"testing"
)

func TestPrimaryIndexEntry(t *testing.T) {
	buf := make([]byte, 0, 300)
	pk1 := []byte("testkey")

	e, _ := NewPrimaryIndexEntry(pk1)

	buf, _ = e.ReadDocId(buf)
	if !bytes.Equal(pk1, buf) {
		t.Errorf("Expected %v, received %v", string(pk1), string(buf))
	}

	buf = buf[:0]
	buf, _ = e.ReadSecKey(buf)
	if !bytes.Equal(nil, buf) {
		t.Errorf("Expected %v, received %v", string(""), string(buf))
	}

	if !bytes.Equal(pk1, e.Bytes()) {
		t.Errorf("Expected %v, received %v", string(pk1), string(e.Bytes()))
	}

	e2, _ := BytesToPrimaryIndexEntry(e.Bytes())

	buf = buf[:0]
	buf, _ = e2.ReadDocId(buf)
	if !bytes.Equal(pk1, buf) {
		t.Errorf("Expected %v, received %v", string(pk1), string(buf))
	}

	buf = buf[:0]
	buf, _ = e2.ReadSecKey(buf)
	if !bytes.Equal(nil, buf) {
		t.Errorf("Expected %v, received %v", string(""), string(buf))
	}

	if !bytes.Equal(pk1, e2.Bytes()) {
		t.Errorf("Expected %v, received %v", string(pk1), string(e.Bytes()))
	}
}

func TestSecondaryIndexEntry(t *testing.T) {
	buf := make([]byte, 0, 300)
	docid := []byte("doc-1")
	sk1 := []byte(`["field1","field2"]`)
	sk2 := []byte(`["field1",]`)
	sk3 := []byte(`[]`)

	e, _ := NewSecondaryIndexEntry(sk1, docid)

	buf, _ = e.ReadDocId(buf)
	if !bytes.Equal(docid, buf) {
		t.Errorf("Expected %v, received %v", string(docid), string(buf))
	}

	buf = buf[:0]
	buf, _ = e.ReadSecKey(buf)
	if !bytes.Equal(sk1, buf) {
		t.Errorf("Expected %v, received %v", string(sk1), string(buf))
	}

	e2, _ := BytesToSecondaryIndexEntry(e.Bytes())

	buf = buf[:0]
	buf, _ = e2.ReadDocId(buf)
	if !bytes.Equal(docid, buf) {
		t.Errorf("Expected %v, received %v", string(docid), string(buf))
	}

	buf = buf[:0]
	buf, _ = e2.ReadSecKey(buf)
	if !bytes.Equal(sk1, buf) {
		t.Errorf("Expected %v, received %v", string(sk1), string(buf))
	}

	if !bytes.Equal(e.Bytes(), e2.Bytes()) {
		t.Errorf("Expected %v, received %v", string(e.Bytes()), string(e2.Bytes()))
	}

	_, err := NewSecondaryIndexEntry(sk2, docid)
	if err == nil {
		t.Errorf("Expected error")
	}

	_, err = NewSecondaryIndexEntry(sk3, docid)
	if err != ErrSecKeyNil {
		t.Errorf("Expected error")
	}
}

func TestPrimaryIndexEntryMatch(t *testing.T) {
	e1, _ := NewPrimaryIndexEntry([]byte("prefixmatch"))
	k1, _ := NewPrimaryKey([]byte("prefix"))
	k2, _ := NewPrimaryKey([]byte("prefixmatch"))

	if k1.Compare(e1) == 0 {
		t.Errorf("Expected mismatch")
	}

	if k2.Compare(e1) != 0 {
		t.Errorf("Expected match")
	}
}

func TestSecondaryIndexEntryMatch(t *testing.T) {
	e1, _ := NewSecondaryIndexEntry([]byte(`["key1"]`), []byte("doc1"))
	e2, _ := NewSecondaryIndexEntry([]byte(`["key1","key2"]`), []byte("doc1"))
	e3, _ := NewSecondaryIndexEntry([]byte(`["key1","key2","key3"]`), []byte("doc1"))
	e4, _ := NewSecondaryIndexEntry([]byte(`["partialmatch"]`), []byte("doc1"))

	k1, _ := NewSecondaryKey([]byte(`["key1"]`))
	k2, _ := NewSecondaryKey([]byte(`["key1","key2"]`))
	k3, _ := NewSecondaryKey([]byte(`["partial"]`))

	if k1.Compare(e1) != 0 {
		t.Errorf("Expected match")
	}

	if k1.ComparePrefixFields(e1) != 0 {
		t.Errorf("Expected match")
	}

	if k1.Compare(e2) == 0 {
		t.Errorf("Expected mismatch")
	}

	if k1.ComparePrefixFields(e2) != 0 {
		t.Errorf("Expected match")
	}

	if k1.ComparePrefixFields(e3) != 0 {
		t.Errorf("Expected match")
	}

	if k2.Compare(e1) == 0 {
		t.Errorf("Expected mismatch")
	}

	if k2.ComparePrefixFields(e1) == 0 {
		t.Errorf("Expected mismatch")
	}

	if k2.Compare(e2) != 0 {
		t.Errorf("Expected match")
	}

	if k2.ComparePrefixFields(e2) != 0 {
		t.Errorf("Expected match")
	}

	if k2.Compare(e3) == 0 {
		t.Errorf("Expected mismatch")
	}

	if k2.ComparePrefixFields(e3) != 0 {
		t.Errorf("Expected match")
	}

	if k3.ComparePrefixFields(e4) == 0 {
		t.Errorf("Expected mismatch")
	}
}
