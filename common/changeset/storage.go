package changeset

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"sort"
)

const (
	DefaultIncarnation      = ^uint64(1)
	storageEnodingIndexSize = 4
	storageEnodingStartElem = uint32(4)
	storageEnodingRowSize   = uint32(common.HashLength * 3)
)

func NewStorageChangeSet() *ChangeSet {
	return &ChangeSet{
		Changes: make([]Change, 0),
		keyLen:  2*common.HashLength + common.IncarnationLength,
	}
}

/*
Storage ChangeSet is serialized in the following manner in order to facilitate binary search:
4:[32:32:32]:[4:8]
numOfElements:[addrHash:keyHash:value]:[elementNum:incarnation]

1. The number of keys N (uint32, 4 bytes).
2. Contiguous array of [addrHash(32b)+keyHash(32b)+value(32b)] (N*(32+32+32) bytes).
3. Contiguous array of not default incarnations, like: index of change(uint64) + incarnation(uint64)

uint32 integers are serialized as big-endian.
*/
func EncodeStorage(s *ChangeSet) ([]byte, error) {
	sort.Sort(s)
	buf := new(bytes.Buffer)
	intArr := make([]byte, 4)
	n := s.Len()
	binary.BigEndian.PutUint32(intArr, uint32(n))
	_, err := buf.Write(intArr)
	if err != nil {
		return nil, err
	}

	notDefaultIncarnationList := make([]struct {
		ID  uint8
		Inc uint64
	}, 0)
	storageKey := make([]byte, 32+32)
	var lenOfVals uint32
	storageVals := make([]uint8, len(s.Changes))
	for i := 0; i < n; i++ {
		//copy addrHash
		copy(
			storageKey[0:common.HashLength],
			s.Changes[i].Key[0:common.HashLength],
		)
		//copy key
		copy(
			storageKey[common.HashLength:2*common.HashLength],
			s.Changes[i].Key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
		)
		storageVals[i]=uint8(len(s.Changes[i].Value))
		lenOfVals+=uint32(len(s.Changes[i].Value))
		_, err = buf.Write(storageKey)
		if err != nil {
			return nil, err
		}

		incarnation := binary.BigEndian.Uint64(s.Changes[i].Key[common.HashLength : common.HashLength+common.IncarnationLength])
		if incarnation != DefaultIncarnation {
			notDefaultIncarnationList = append(notDefaultIncarnationList, struct {
				ID  uint8
				Inc uint64
			}{ID: uint8(i), Inc: incarnation})
		}
	}
	lenOfVals+=uint32(len(storageVals))

	bb:=make([]byte, 4)
	binary.LittleEndian.PutUint32(bb, lenOfVals)
	_, err = buf.Write(bb)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(storageVals)
	if err != nil {
		return nil, err
	}
	for i:=0;i<n;i++ {
		_, err = buf.Write(s.Changes[i].Value)
		if err != nil {
			return nil, err
		}
	}
	if len(notDefaultIncarnationList) > 0 {
		b := make([]byte, storageEnodingIndexSize+common.IncarnationLength)
		for _, v := range notDefaultIncarnationList {
			binary.BigEndian.PutUint32(b[0:storageEnodingIndexSize], uint32(v.ID))
			binary.BigEndian.PutUint64(b[storageEnodingIndexSize:storageEnodingIndexSize+common.IncarnationLength], v.Inc)
			_, err = buf.Write(b)
			if err != nil {
				return nil, err
			}
		}
	}

	byt := buf.Bytes()
	return byt, nil
}

func DecodeStorage(b []byte) (*ChangeSet, error) {
	h := NewStorageChangeSet()
	if len(b) == 0 {
		h.Changes = make([]Change, 0)
		return h, nil
	}

	if len(b) < 4 {
		return h, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfElements := binary.BigEndian.Uint32(b[0:4])
	h.Changes = make([]Change, numOfElements)

	if numOfElements == 0 {
		return h, nil
	}

	lenOfValsPos:=storageEnodingStartElem + numOfElements*(2*common.HashLength)
	lenOfVals := binary.LittleEndian.Uint32(b[lenOfValsPos:lenOfValsPos+4])
	fmt.Println("lenOfVals", lenOfVals)
	incarnationPosition := storageEnodingStartElem + numOfElements*(2*common.HashLength)+4+lenOfVals
	if uint32(len(b)) < incarnationPosition {
		fmt.Println("DecodeStorage")
		return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), incarnationPosition)
	}

	//parse not default incarnations
	incarnationsLength := len(b[incarnationPosition:])
	notDefaultIncarnation := make(map[uint32]uint64, 0)
	var (
		id  uint32
		inc uint64
		ok  bool
	)

	if incarnationsLength > 0 {
		if incarnationsLength%(storageEnodingIndexSize+common.IncarnationLength) != 0 {
			return h, fmt.Errorf("decode: incarnatin part is incorrect(%d bytes)", len(b[incarnationPosition:]))
		}
		numOfIncarnations := incarnationsLength / (storageEnodingIndexSize + common.IncarnationLength)
		for i := 0; i < numOfIncarnations; i++ {
			id = binary.BigEndian.Uint32(b[incarnationPosition : incarnationPosition+4])
			inc = binary.BigEndian.Uint64(b[incarnationPosition+4 : incarnationPosition+4+8])
			notDefaultIncarnation[id] = inc
			incarnationPosition += (storageEnodingIndexSize + common.IncarnationLength)
		}
	}

	elementStart := uint32(4)
	key := make([]byte, common.HashLength*2+common.IncarnationLength)
	valPos:=lenOfValsPos+4+numOfElements
	for i := uint32(0); i < numOfElements; i++ {
		//copy addrHash
		elem:=elementStart+i*common.HashLength*2
		copy(key[0:common.HashLength], b[elem:elem+common.HashLength])
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			b[elem+common.HashLength:elem+2*common.HashLength],
		)
		//set incarnation
		if inc, ok = notDefaultIncarnation[i]; ok {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], inc)
		} else {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], DefaultIncarnation)
		}
		valLen:=b[lenOfValsPos+4+uint32(i)]
		fmt.Println("valLen",valLen)

		h.Changes[i].Key = common.CopyBytes(key)
		fmt.Println("valuePos", valPos, valPos+uint32(valLen))
		h.Changes[i].Value =common.CopyBytes(b[valPos:valPos+uint32(valLen)])
		valPos+=uint32(valLen)
	}

	return h, nil
}

type StorageChangeSetBytes []byte

func (b StorageChangeSetBytes) Walk(f func(k, v []byte) error) error {
	if len(b) == 0 {
		return nil
	}
	if len(b) < 4 {
		return fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfItems := binary.BigEndian.Uint32(b[0:4])

	if numOfItems == 0 {
		return nil
	}

	incarnationPosition := storageEnodingStartElem + numOfItems*(3*common.HashLength)
	if uint32(len(b)) < incarnationPosition {
		fmt.Println("WalkStorage", numOfItems)
		return fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), incarnationPosition)
	}
	incarnationsLength := len(b[incarnationPosition:])
	notDefaultIncarnation := make(map[uint32]uint64, 0)
	var (
		id  uint32
		inc uint64
		ok  bool
	)

	if incarnationsLength > 0 {
		if incarnationsLength%(storageEnodingIndexSize+common.IncarnationLength) != 0 {
			return fmt.Errorf("decode: incarnatin part is incorrect(%d bytes)", len(b[incarnationPosition:]))
		}
		numOfIncarnations := incarnationsLength / (storageEnodingIndexSize + common.IncarnationLength)
		for i := 0; i < numOfIncarnations; i++ {
			id = binary.BigEndian.Uint32(b[incarnationPosition : incarnationPosition+storageEnodingIndexSize])
			inc = binary.BigEndian.Uint64(b[incarnationPosition+storageEnodingIndexSize : incarnationPosition+storageEnodingIndexSize+common.IncarnationLength])
			notDefaultIncarnation[id] = inc
			incarnationPosition += (storageEnodingIndexSize + common.IncarnationLength)
		}
	}

	key := make([]byte, common.HashLength*2+common.IncarnationLength)
	for i := uint32(0); i < numOfItems; i++ {
		//copy addrHash
		copy(key[0:common.HashLength], b[storageEnodingStartElem+storageEnodingRowSize*i:storageEnodingStartElem+storageEnodingRowSize*i+common.HashLength])
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			b[storageEnodingStartElem+storageEnodingRowSize*i+common.HashLength:storageEnodingStartElem+storageEnodingRowSize*i+2*common.HashLength],
		)
		//set incarnation
		if inc, ok = notDefaultIncarnation[i]; ok {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], inc)
		} else {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], DefaultIncarnation)
		}

		err := f(common.CopyBytes(key), common.CopyBytes(b[storageEnodingStartElem+storageEnodingRowSize*i+2*common.HashLength:storageEnodingStartElem+storageEnodingRowSize*i+3*common.HashLength]))
		if err != nil {
			return err
		}
	}
	return nil
}

func (b StorageChangeSetBytes) FindLast(k []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfItems := binary.BigEndian.Uint32(b[0:4])

	if numOfItems == 0 {
		return nil, nil
	}

	incarnationPosition := storageEnodingStartElem + numOfItems*(3*common.HashLength)
	if uint32(len(b)) < incarnationPosition {
		fmt.Println("FindLast storage")
		return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), incarnationPosition)
	}

	incarnationsLength := len(b[incarnationPosition:])
	notDefaultIncarnation := make(map[uint32]uint64, 0)
	var (
		id  uint32
		inc uint64
		ok  bool
	)

	if incarnationsLength > 0 {
		if incarnationsLength%(storageEnodingIndexSize+common.IncarnationLength) != 0 {
			return nil, fmt.Errorf("decode: incarnatin part is incorrect(%d bytes)", len(b[incarnationPosition:]))
		}
		numOfIncarnations := incarnationsLength / (storageEnodingIndexSize + common.IncarnationLength)
		for i := 0; i < numOfIncarnations; i++ {
			id = binary.BigEndian.Uint32(b[incarnationPosition : incarnationPosition+storageEnodingIndexSize])
			inc = binary.BigEndian.Uint64(b[incarnationPosition+storageEnodingIndexSize : incarnationPosition+storageEnodingIndexSize+common.IncarnationLength])
			notDefaultIncarnation[id] = inc
			incarnationPosition += (storageEnodingIndexSize + common.IncarnationLength)
		}
	}

	key := make([]byte, common.HashLength*2+common.IncarnationLength)
	for i := numOfItems - 1; int(i) >= 0; i-- {
		//copy addrHash
		copy(key[0:common.HashLength], b[storageEnodingStartElem+storageEnodingRowSize*i:storageEnodingStartElem+storageEnodingRowSize*i+common.HashLength])
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			b[storageEnodingStartElem+storageEnodingRowSize*i+common.HashLength:storageEnodingStartElem+storageEnodingRowSize*i+2*common.HashLength],
		)
		//set incarnation
		if inc, ok = notDefaultIncarnation[i]; ok {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], inc)
		} else {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], DefaultIncarnation)
		}

		if bytes.Equal(key, k) {
			return common.CopyBytes(b[storageEnodingStartElem+storageEnodingRowSize*i+2*common.HashLength : storageEnodingStartElem+storageEnodingRowSize*i+3*common.HashLength]), nil
		}
	}
	return nil, errors.New("not found")
}