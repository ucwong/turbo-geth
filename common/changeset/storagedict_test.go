package changeset

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestEncodingStorageDictWithoutNotDefaultIncarnation(t *testing.T) {
	// empty StorageChangeSset first
	ch := NewStorageChangeSet()
	_, err := EncodeStorage(ch)
	assert.NoError(t, err)

	numOfElements := 2
	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	b, err := EncodeStorageDict(ch)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Size Diff", len(b), len(b2))

	ch2, err := DecodeStorageDict(b)
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

func TestEncodingStorageDict2WithtRandomIncarnation(t *testing.T) {
	// empty StorageChangeSet first
	ch := NewStorageChangeSet()
	_, err := EncodeStorageDict2(ch)
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

	fmt.Println("encode")
	b, err := EncodeStorageDict2(ch)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := EncodeStorageDict(ch)
	if err != nil {
		t.Fatal(err)
	}
	b3, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(len(b))
	t.Log(len(b2))
	t.Log(len(b3))

	fmt.Println("decode")
	ch2, err := DecodeStorageDict2(b)
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

func TestEncodingStorageDict2WithoutNotDefaultIncarnation(t *testing.T) {
	// empty StorageChangeSset first
	ch := NewStorageChangeSet()
	_, err := EncodeStorage(ch)
	assert.NoError(t, err)

	numOfElements := 2
	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	b, err := EncodeStorageDict(ch)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := EncodeStorage(ch)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Size Diff", len(b), len(b2))

	ch2, err := DecodeStorageDict(b)
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

func TestEncodingStorageDictWithtRandomIncarnation(t *testing.T) {
	// empty StorageChangeSet first
	ch := NewStorageChangeSet()
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

	b, err := EncodeStorageDict(ch)
	if err != nil {
		t.Fatal(err)
	}

	ch2, err := DecodeStorageDict(b)
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