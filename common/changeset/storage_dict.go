package changeset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"sort"
)

/*
Storage ChangeSet is serialized in the following manner in order to facilitate binary search:
4:[32:32:32]:[4:8]
numOfElements:[addrHash:keyHash:value]:[elementNum:incarnation]

1. The number of keys N (uint32, 4 bytes).
2. Contiguous array of addrHash(32b)+keyHash(32b)+value(32b)] (N*32 bytes).
2. Contiguous array of [addrHash(32b)+keyHash(32b)+value(32b)] (N*32 bytes).
3. Contiguous array of not default incarnations, like: index of change(uint64) + incarnation(uint64)

uint32 integers are serialized as big-endian.
*/
func EncodeStorageDict(s *ChangeSet) ([]byte, error) {
	sort.Sort(s)
	buf := new(bytes.Buffer)
	intArr := make([]byte, 4)
	n := s.Len()
	binary.LittleEndian.PutUint32(intArr, uint32(n))
	_, err := buf.Write(intArr)
	if err != nil {
		return nil, err
	}

	addrHashesMap := make(map[common.Hash]uint16, 0)
	addrHashList := make([]common.Hash,0)
	keyHashesMap := make(map[common.Hash]uint16, 0)
	keyHashesList := make([]common.Hash,0)


	notDefaultIncarnationList := make([]struct {
		ID  uint8
		Inc uint64
	}, 0)
	lenOfStorageIndex:=2
	storageKeys := make([][]byte, len(s.Changes))

	var lenOfVals uint32
	storageVals := make([]uint8, len(s.Changes))
	nextIDAddrHash :=uint16(0)
	nextIDKeyHash :=uint16(0)
	for i := 0; i < n; i++ {
		//copy addrHash
		addrHash:=common.Hash{}
		keyHash:=common.Hash{}
		copy(
			addrHash[:],
			s.Changes[i].Key[0:common.HashLength],
		)

		idAddr, ok:= addrHashesMap[addrHash]
		if !ok {
			addrHashesMap[addrHash]=nextIDAddrHash
			idAddr = nextIDAddrHash
			nextIDAddrHash++
			addrHashList=append(addrHashList, addrHash)
		}

		//copy key
		copy(
			keyHash[:],
			s.Changes[i].Key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
		)

		idKey, ok:= keyHashesMap[addrHash]
		if !ok {
			keyHashesMap[keyHash]=nextIDKeyHash
			idKey = nextIDKeyHash
			nextIDKeyHash++
			keyHashesList=append(keyHashesList, keyHash)
		}

		storageVals[i]=uint8(len(s.Changes[i].Value))
		lenOfVals+=uint32(len(s.Changes[i].Value))

		storageKey:=make([]byte, lenOfStorageIndex+lenOfStorageIndex)
		binary.LittleEndian.PutUint16(storageKey[0:lenOfStorageIndex],idAddr)
		binary.LittleEndian.PutUint16(storageKey[lenOfStorageIndex:2*lenOfStorageIndex],idKey)
		storageKeys=append(storageKeys, storageKey)

		incarnation := binary.LittleEndian.Uint64(s.Changes[i].Key[common.HashLength : common.HashLength+common.IncarnationLength])
		if incarnation != DefaultIncarnation {
			notDefaultIncarnationList = append(notDefaultIncarnationList, struct {
				ID  uint8
				Inc uint64
			}{ID: uint8(i), Inc: incarnation})
		}
	}

	lenOfAddHashes := make([]byte, 2)
	binary.LittleEndian.PutUint16(lenOfAddHashes, uint16(len(addrHashList)))
	_, err = buf.Write(lenOfAddHashes)
	if err != nil {
		return nil, err
	}
	for _,v:=range addrHashList {
		_, err = buf.Write(v.Bytes())
		if err != nil {
			return nil, err
		}
	}

	binary.LittleEndian.PutUint16(lenOfAddHashes, uint16(len(keyHashesList)))
	_, err = buf.Write(lenOfAddHashes)
	if err != nil {
		return nil, err
	}
	for _,v:=range keyHashesList {
		_, err = buf.Write(v.Bytes())
		if err != nil {
			return nil, err
		}
	}
	for _,v:=range storageKeys {
		_, err = buf.Write(v)
		if err != nil {
			return nil, err
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
			binary.LittleEndian.PutUint32(b[0:storageEnodingIndexSize], uint32(v.ID))
			binary.LittleEndian.PutUint64(b[storageEnodingIndexSize:storageEnodingIndexSize+common.IncarnationLength], v.Inc)
			_, err = buf.Write(b)
			if err != nil {
				return nil, err
			}
		}
	}

	byt := buf.Bytes()
	return byt, nil
}


func DecodeStorageDict(b []byte) (*ChangeSet, error) {
	h := NewStorageChangeSet()
	if len(b) == 0 {
		h.Changes = make([]Change, 0)
		return h, nil
	}
	if len(b) < 4 {
		return h, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	numOfElements := binary.LittleEndian.Uint32(b[0:4])
	h.Changes = make([]Change, numOfElements)

	if numOfElements == 0 {
		return h, nil
	}

	dictLen:=binary.LittleEndian.Uint16(b[4:6])
	addMap:=make(map[uint16]common.Hash)
	for i:=0; i< int(dictLen); i++ {
		addMap[uint16(i)] = common.BytesToHash(b[6+i*common.HashLength:6+i*common.HashLength+common.HashLength])
	}

	dictKeysStart:=6+dictLen*common.HashLength
	dictKeyLen:=binary.LittleEndian.Uint16(b[dictKeysStart:dictKeysStart+2])

	keyMap:=make(map[uint16]common.Hash)
	for i:=0; i< int(dictKeyLen); i++ {
		keyMap[uint16(i)] = common.BytesToHash(b[int(dictKeysStart)+2+i*common.HashLength:int(dictKeysStart)+2+i*common.HashLength+common.HashLength])
	}

	lenOfValsPos:=storageEnodingStartElem + 2+uint32(dictLen)*common.HashLength + 2+ uint32(dictKeyLen)*common.HashLength + numOfElements*4
	lenOfVals := binary.LittleEndian.Uint32(b[lenOfValsPos:lenOfValsPos+4])

	incarnationPosition := lenOfValsPos+4+lenOfVals
	if uint32(len(b)) < incarnationPosition {
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
			id = binary.LittleEndian.Uint32(b[incarnationPosition : incarnationPosition+4])
			inc = binary.LittleEndian.Uint64(b[incarnationPosition+4 : incarnationPosition+4+8])
			notDefaultIncarnation[id] = inc
			incarnationPosition += (storageEnodingIndexSize + common.IncarnationLength)
		}
	}

	elementStart := storageEnodingStartElem + 2+uint32(dictLen)*common.HashLength + 2+ uint32(dictKeyLen)*common.HashLength
	key := make([]byte, common.HashLength*2+common.IncarnationLength)
	valPos:=lenOfValsPos+4+numOfElements
	for i := uint32(0); i < numOfElements; i++ {
		//copy addrHash
		elem:=elementStart+i*4
		copy(
			key[0:common.HashLength],
			addMap[binary.LittleEndian.Uint16(b[elem:elem+2])].Bytes(),
			)
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			keyMap[binary.LittleEndian.Uint16(b[elem+2:elem+4])].Bytes(),
		)
		//set incarnation
		if inc, ok = notDefaultIncarnation[i]; ok {
			binary.LittleEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], inc)
		} else {
			binary.LittleEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], DefaultIncarnation)
		}
		valLen:=b[lenOfValsPos+4+uint32(i)]


		h.Changes[i].Key = common.CopyBytes(key)
		h.Changes[i].Value =common.CopyBytes(b[valPos:valPos+uint32(valLen)])
		valPos+=uint32(valLen)
	}

	return h, nil
}