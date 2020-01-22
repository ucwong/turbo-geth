package changeset

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
)

func createTestChangeSet() []byte {
	// empty AccountChangeSet first
	ch := NewChangeSet()
	// add some entries
	_ = ch.Add(common.FromHex("56fb07ee"), common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"))
	_ = ch.Add(common.FromHex("a5e4c9a1"), common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"))
	_ = ch.Add(common.FromHex("22bb06f4"), common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"))
	encoded, _ := EncodeChangeSet(ch)
	return encoded
}

func TestEncoding(t *testing.T) {
	// empty AccountChangeSet first
	ch := NewChangeSet()
	_, err := EncodeChangeSet(ch)
	assert.NoError(t, err)

	// add some entries
	err = ch.Add(common.FromHex("56fb07ee"), common.FromHex("f7f6db1eb17c6d582078e0ffdd0c"))
	assert.NoError(t, err)
	err = ch.Add(common.FromHex("a5e4c9a1"), common.FromHex("b1e9b5c16355eede662031dd621d08faf4ea"))
	assert.NoError(t, err)
	err = ch.Add(common.FromHex("22bb06f4"), common.FromHex("862cf52b74f1cea41ddd8ffa4b3e7c7790"))
	assert.NoError(t, err)
}

const (
	numOfElements      = 30
	defaultIncarnation = 1
)

func TestEncodingStorageWithoutNotDefaultIncarnation(t *testing.T) {
	// empty StorageChangeSset first
	ch := NewChangeSet()
	_, err := EncodeStorage(ch)
	assert.NoError(t, err)

	numOfElements := 10
	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	b, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}

	ch2, err := DecodeStorage(b)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(ch, ch2) {
		for i, v := range ch.Changes {
			if !bytes.Equal(v.Key, ch2.Changes[i].Key) || !bytes.Equal(v.Value, ch2.Changes[i].Value) {
				fmt.Println("Diff ", i)
				fmt.Println("k1", common.Bytes2Hex(v.Key), len(v.Key))
				fmt.Println("k2", common.Bytes2Hex(ch2.Changes[i].Key))
				fmt.Println("v1", common.Bytes2Hex(v.Value))
				fmt.Println("v2", common.Bytes2Hex(ch2.Changes[i].Value))
			}
		}
		t.Fatal("not equal")
	}

}

func TestEncodingStorageWithtRandomIncarnation(t *testing.T) {
	// empty StorageChangeSet first
	ch := NewChangeSet()
	_, err := EncodeStorage(ch)
	assert.NoError(t, err)

	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, rand.Uint64(), key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	b, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}

	ch2, err := DecodeStorage(b)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(ch, ch2) {
		for i, v := range ch.Changes {
			if !bytes.Equal(v.Key, ch2.Changes[i].Key) || !bytes.Equal(v.Value, ch2.Changes[i].Value) {
				fmt.Println("Diff ", i)
				fmt.Println("k1", common.Bytes2Hex(v.Key), len(v.Key))
				fmt.Println("k2", common.Bytes2Hex(ch2.Changes[i].Key))
				fmt.Println("v1", common.Bytes2Hex(v.Value))
				fmt.Println("v2", common.Bytes2Hex(ch2.Changes[i].Value))
			}
		}
		t.Fatal("not equal")
	}
}

func TestFindLast(t *testing.T) {
	encoded := createTestChangeSet()
	val, err := FindLast(encoded, common.FromHex("56fb07ee"))
	assert.NoError(t, err)
	if !bytes.Equal(val, common.FromHex("f7f6db1eb17c6d582078e0ffdd0c")) {
		t.Error("Invalid value")
	}
}
