package changeset

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

func TestEncodingStorageDict3WithoutNotDefaultIncarnation(t *testing.T) {
	// empty StorageChangeSset first
	ch := NewStorageChangeSet()
	_, err := EncodeStorage(ch)
	assert.NoError(t, err)

	numOfElements := 5
	for i := 0; i < numOfElements; i++ {
		addrHash, _ := common.HashData([]byte("addrHash" + strconv.Itoa(i)))
		key, _ := common.HashData([]byte("key" + strconv.Itoa(i)))
		val, _ := common.HashData([]byte("val" + strconv.Itoa(i)))
		err = ch.Add(dbutils.GenerateCompositeStorageKey(addrHash, defaultIncarnation, key), val.Bytes())
		if err != nil {
			t.Fatal(err)
		}
	}

	//b, err := EncodeStorageDict2(ch)
	//if err != nil {
	//	t.Fatal(err)
	//}
	b3, err := EncodeStorageDict3(ch)
	if err != nil {
		t.Fatal(err)
	}
	//b2, err := EncodeStorage(ch)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//fmt.Println("Size Diff", len(b), len(b3), len(b2))

	ch2, err := DecodeStorageDict3(b3)
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

func TestEncodingStorageDict3WithtRandomIncarnation(t *testing.T) {
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

	b, err := EncodeStorageDict3(ch)
	if err != nil {
		t.Fatal(err)
	}

	ch2, err := DecodeStorageDict3(b)
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

func TestEncodingStorageDict3WithtRandomIncarnation1(t *testing.T) {
	db, err := ethdb.NewBoltDatabase("/media/b00ris/ssd/ethchain/thin_1/geth/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	v, err:=db.Get(dbutils.ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(4048739), dbutils.StorageHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}
	cs,err :=DecodeChangeSet(v)
	t.Log(len(cs.Changes))
	if err!=nil {
		t.Fatal(err)
	}

	b, err := EncodeStorageDict3(cs)
	if err != nil {
		t.Fatal(err)
	}

	ch2, err := DecodeStorageDict3(b)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(cs.Changes, ch2.Changes) {
		for i, v := range cs.Changes {
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

func TestEncodingStorageDict3WithtRandomIncarnation2(t *testing.T) {
	db, err := ethdb.NewBoltDatabase("/home/b00ris/chaindata")
	if err != nil {
		log.Fatal(err)
	}

	v, err:=db.Get(dbutils.ChangeSetBucket, dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(290143), dbutils.StorageHistoryBucket))
	if err!=nil {
		t.Fatal(err)
	}
	cs,err :=DecodeChangeSet(v)
	//t.Log(len(cs.Changes))
	if err!=nil {
		t.Fatal(err)
	}

	b, err := EncodeStorageDict3(cs)
	if err != nil {
		t.Fatal(err)
	}

	ch2, err := DecodeStorageDict3(b)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(cs.Changes, ch2.Changes) {
		for i, v := range cs.Changes {
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