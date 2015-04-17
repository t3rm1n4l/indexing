package indexer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/couchbase/indexing/secondary/collatejson"
)

var (
	ErrSecKeyNil = errors.New("Secondary key array is empty")
)

// Generic index entry abstraction (primary or secondary)
// Represents a row in the index
type IndexEntry interface {
	ReadDocId([]byte) ([]byte, error)
	ReadSecKey([]byte) ([]byte, error)

	Bytes() []byte
	String() string
}

// Generic index key abstraction (primary or secondary)
// Represents a key supplied by the user for scan operation
type IndexKey interface {
	Compare(IndexEntry) int
	ComparePrefixFields(IndexEntry) int
	Bytes() []byte
	String() string
}

// Storage encoding for primary index entry
// Raw docid bytes are stored as the key
type primaryIndexEntry []byte

func NewPrimaryIndexEntry(docid []byte) (*primaryIndexEntry, error) {
	buf := append([]byte(nil), docid...)
	e := primaryIndexEntry(buf)
	return &e, nil
}

func BytesToPrimaryIndexEntry(b []byte) (*primaryIndexEntry, error) {
	e := primaryIndexEntry(b)
	return &e, nil
}

func (e *primaryIndexEntry) ReadDocId(buf []byte) ([]byte, error) {
	buf = append(buf, []byte(*e)...)
	return buf, nil
}

func (e *primaryIndexEntry) ReadSecKey(buf []byte) ([]byte, error) {
	return nil, nil
}

func (e *primaryIndexEntry) Bytes() []byte {
	return []byte(*e)
}

func (e *primaryIndexEntry) String() string {
	return string(*e)
}

// Storage encoding for secondary index entry
// Format:
// [collate_json_encoded_sec_key][raw_docid_bytes][len_of_docid_2_bytes]
type secondaryIndexEntry []byte

func NewSecondaryIndexEntry(key []byte, docid []byte) (*secondaryIndexEntry, error) {
	var err error
	if bytes.Equal([]byte("[]"), key) {
		return nil, ErrSecKeyNil
	}

	buf := make([]byte, 0, MAX_SEC_KEY_LEN+len(docid)+2)
	codec := collatejson.NewCodec(16)
	if buf, err = codec.Encode(key, buf); err != nil {
		return nil, err
	}

	buf = append(buf, docid...)
	buf = buf[:len(buf)+2]
	offset := len(buf) - 2
	binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(docid)))

	buf = append([]byte(nil), buf[:len(buf)]...)
	e := secondaryIndexEntry(buf)
	return &e, nil
}

func BytesToSecondaryIndexEntry(b []byte) (*secondaryIndexEntry, error) {
	e := secondaryIndexEntry(b)
	return &e, nil
}

func (e *secondaryIndexEntry) lenDocId() int {
	rbuf := []byte(*e)
	offset := len(rbuf) - 2
	l := binary.LittleEndian.Uint16(rbuf[offset : offset+2])
	return int(l)
}

func (e *secondaryIndexEntry) ReadDocId(buf []byte) ([]byte, error) {
	rbuf := []byte(*e)
	doclen := e.lenDocId()
	offset := len(rbuf) - doclen - 2
	buf = append(buf, rbuf[offset:offset+doclen]...)

	return buf, nil
}

func (e *secondaryIndexEntry) ReadSecKey(buf []byte) ([]byte, error) {
	var err error
	rbuf := []byte(*e)
	doclen := e.lenDocId()

	codec := collatejson.NewCodec(16)
	encoded := rbuf[0 : len(rbuf)-doclen-2]
	if buf, err = codec.Decode(encoded, buf); err != nil {
		return nil, err
	}

	return buf, nil
}

func (e *secondaryIndexEntry) Bytes() []byte {
	return []byte(*e)
}

func (e *secondaryIndexEntry) String() string {
	buf := make([]byte, MAX_SEC_KEY_LEN*4)
	buf, _ = e.ReadSecKey(buf)
	buf = append(buf, ':')
	buf, _ = e.ReadDocId(buf)

	return string(buf)
}

type NilIndexKey []byte

func (k *NilIndexKey) Compare(entry IndexEntry) int {
	return -1
}

func (k *NilIndexKey) ComparePrefixFields(entry IndexEntry) int {
	return 0
}

func (k *NilIndexKey) Bytes() []byte {
	return nil
}

func (k *NilIndexKey) String() string {
	return "nil"
}

type primaryKey []byte

func NewPrimaryKey(docid []byte) (IndexKey, error) {
	if len(docid) == 0 {
		return &NilIndexKey{}, nil
	}
	k := primaryKey(docid)
	return &k, nil
}

func (k *primaryKey) Compare(entry IndexEntry) int {
	return bytes.Compare(*k, entry.Bytes())
}

func (k *primaryKey) ComparePrefixFields(entry IndexEntry) int {
	kbytes := []byte(*k)
	klen := len(kbytes)

	if klen > len(entry.Bytes()) {
		return 1
	}

	return bytes.Compare(*k, entry.Bytes()[:klen])
}

func (k *primaryKey) Bytes() []byte {
	return *k
}

func (k *primaryKey) String() string {
	return string(*k)
}

type secondaryKey []byte

func NewSecondaryKey(key []byte) (IndexKey, error) {
	if len(key) == 0 {
		return &NilIndexKey{}, nil
	}

	var err error
	buf := make([]byte, 0, MAX_SEC_KEY_LEN)
	codec := collatejson.NewCodec(16)
	if buf, err = codec.Encode(key, buf); err != nil {
		return nil, err
	}

	buf = append([]byte(nil), buf[:len(buf)]...)

	k := secondaryKey(buf)
	return &k, nil
}

func (k *secondaryKey) Compare(entry IndexEntry) int {
	kbytes := []byte(*k)
	klen := len(kbytes)

	if klen > len(entry.Bytes()) {
		return 1
	}

	return bytes.Compare(kbytes[:klen], entry.Bytes()[:klen])
}

// A compound secondary index entry would be an array
// of indexed fields. A compound index on N fields
// also should be equivalent to other indexes with 1 to N
// prefix fields.
// When a user supplies a low or high key constraints,
// indexing systems should check the number of fields
// in the user supplied key and treat target index as
// an index created on that n prefix fields.
//
// Collatejson encoding puts a terminator character at the
// end of encoded bytes to represent termination of json array.
// Since we want to match partial secondary keys against fullset
// compound index entries, we would remove the last byte from the
// encoded user supplied secondary key to match prefixes.
func (k *secondaryKey) ComparePrefixFields(entry IndexEntry) int {
	kbytes := []byte(*k)
	klen := len(kbytes)
	if klen > len(entry.Bytes()) {
		return 1
	}
	return bytes.Compare(kbytes[:klen-1], entry.Bytes()[:klen-1])
}

func (k *secondaryKey) Bytes() []byte {
	return *k
}

func (k *secondaryKey) String() string {
	buf := make([]byte, 0, MAX_SEC_KEY_LEN)
	codec := collatejson.NewCodec(16)
	buf, _ = codec.Decode(*k, buf)
	return string(buf)
}
