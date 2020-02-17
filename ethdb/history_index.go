package ethdb

import (
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"sort"
)

//
type HistoryIndex []uint64

func (hi *HistoryIndex) Encode() ([]byte, error) {
	return rlp.EncodeToBytes(hi)
}

func (hi *HistoryIndex) Decode(s []byte) error {
	if len(s) == 0 {
		return nil
	}
	return rlp.DecodeBytes(s, &hi)
}

func (hi *HistoryIndex) Append(v uint64) *HistoryIndex {
	*hi = append(*hi, v)
	if !sort.SliceIsSorted(*hi, func(i, j int) bool {
		return (*hi)[i] <= (*hi)[j]
	}) {
		sort.Slice(*hi, func(i, j int) bool {
			return (*hi)[i] <= (*hi)[j]
		})
	}

	return hi
}

//most common operation is remove one from the tail
func (hi *HistoryIndex) Remove(v uint64) *HistoryIndex {
	for i := len(*hi) - 1; i >= 0; i-- {
		if (*hi)[i] == v {
			*hi = append((*hi)[:i], (*hi)[i+1:]...)
		}
	}
	return hi
}

func (hi *HistoryIndex) Search(v uint64) (uint64, bool) {
	ln := len(*hi)
	if ln == 0 {
		return 0, false
	}

	if (*hi)[ln-1] < v {
		return 0, false
	}
	for i := ln - 1; i >= 0; i-- {
		if v == (*hi)[i] {
			return v, true
		}

		if (*hi)[i] < v {
			return (*hi)[i+1], true
		}
	}
	return (*hi)[0], true
}

func AppendToIndex(b []byte, timestamp uint64) ([]byte, error) {
	v := new(HistoryIndex)

	if err := v.Decode(b); err != nil {
		return nil, err
	}

	v.Append(timestamp)
	return v.Encode()
}
func RemoveFromIndex(b []byte, timestamp uint64) ([]byte, bool, error) {
	v := new(HistoryIndex)

	if err := v.Decode(b); err != nil {
		return nil, false, err
	}

	v.Remove(timestamp)
	res, err := v.Encode()
	if len(*v) == 0 {
		return res, true, err
	}
	return res, false, err
}

func BoltDBFindByHistory(tx *bolt.Tx, hBucket []byte, key []byte, timestamp uint64) ([]byte, error) {
	//check
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	v, _ := hB.Get(key)
	index := new(HistoryIndex)

	err := index.Decode(v)
	if err != nil {
		return nil, err
	}

	changeSetBlock, ok := index.Search(timestamp)
	if !ok {
		return nil, ErrKeyNotFound
	}

	csB := tx.Bucket(dbutils.ChangeSetBucket)
	if csB == nil {
		return nil, ErrKeyNotFound
	}
	changeSetData, _ := csB.Get(dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(changeSetBlock), hBucket))
	cs, err := dbutils.DecodeChangeSet(changeSetData)
	if err != nil {
		return nil, err
	}

	var data []byte
	data, err = cs.FindLast(key)
	if err != nil {
		return nil, ErrKeyNotFound
	}
	return data, nil

}















const (
	LEN_BYTES = 4
	LenOfUINT32Bytes = 4
	ITEM_LEN = 8
)

func NewHistoryIndex() *HistoryIndexBytes  {
	b:=make(HistoryIndexBytes, LEN_BYTES,16)
	return &b
}

func WrapHistoryIndex(b []byte) *HistoryIndexBytes  {
	index:=HistoryIndexBytes(b)
	if len(index)==0 {
		index=make(HistoryIndexBytes, LEN_BYTES,16)
	}
	return &index
}


type HistoryIndexBytes []byte
func (hi *HistoryIndexBytes) Decode() ([]uint64,error) {
	if hi==nil {
		return []uint64{}, nil
	}
	if len(*hi) <= LEN_BYTES {
		return []uint64{}, nil
	}
	decoded:=make([]uint64, (len(*hi)-LEN_BYTES)/ITEM_LEN)
	for i:=range decoded {
		decoded[i] = binary.LittleEndian.Uint64((*hi)[LEN_BYTES+i*ITEM_LEN:LEN_BYTES+i*ITEM_LEN+ITEM_LEN])
	}
	return decoded, nil
}

func (hi *HistoryIndexBytes) Append(v uint64) *HistoryIndexBytes {
	numOfElements:=binary.LittleEndian.Uint32((*hi)[0:LEN_BYTES])
	b:=make([]byte, ITEM_LEN)
	binary.LittleEndian.PutUint64(b, v)
	*hi = append(*hi, b...)
	binary.LittleEndian.PutUint32((*hi)[0:LEN_BYTES], numOfElements+1)
	return hi
}

func (hi *HistoryIndexBytes) Len() uint32 {
	return binary.LittleEndian.Uint32((*hi)[0:LEN_BYTES])
}

//most common operation is remove one from the tail
func (hi *HistoryIndexBytes) Remove(v uint64) *HistoryIndexBytes {
	numOfElements:=binary.LittleEndian.Uint32((*hi)[0:LEN_BYTES])

	var currentElement uint64

Loop:
	for i := numOfElements; i > 0; i-- {
		elemEnd:=LEN_BYTES+i*8
		currentElement = binary.LittleEndian.Uint64((*hi)[elemEnd-8:elemEnd])
		switch  {
		case currentElement==v:
			*hi=append((*hi)[:elemEnd-ITEM_LEN], (*hi)[elemEnd:]...)
			numOfElements--
		case currentElement<v:
			break Loop
		default:
			continue
		}
	}
	binary.LittleEndian.PutUint32((*hi)[0:LEN_BYTES], numOfElements)
	return hi
}

func (hi *HistoryIndexBytes) Search(v uint64) (uint64, bool) {
	if len(*hi) < 4 {
		fmt.Println(1)
		return 0, false
	}
	ln:=binary.LittleEndian.Uint32((*hi)[0:LEN_BYTES])
	if ln == 0 {
		fmt.Println(2)
		return 0, false
	}

	//check last element
	lastElement:=binary.LittleEndian.Uint64((*hi)[LEN_BYTES+ITEM_LEN*(ln)-ITEM_LEN:LEN_BYTES+ITEM_LEN*(ln)])
	if  lastElement < v {
		return 0, false
	}
	var currentElement uint64
	for i := ln - 1; i > 0; i-- {
		elemEnd:=LEN_BYTES+i*ITEM_LEN
		currentElement = binary.LittleEndian.Uint64((*hi)[elemEnd-ITEM_LEN:elemEnd])
		switch  {
		case currentElement==v:
			return v, true
		case currentElement<v:
			return binary.LittleEndian.Uint64((*hi)[elemEnd:elemEnd+ITEM_LEN]), true
		default:
			continue
		}
	}
	return binary.LittleEndian.Uint64((*hi)[LEN_BYTES:LEN_BYTES+ITEM_LEN]), true
}


