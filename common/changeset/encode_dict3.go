package changeset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"sort"
)

func EncodeStorageDict3(s *ChangeSet) ([]byte, error) {
	sort.Sort(s)
	buf := new(bytes.Buffer)
	int32Arr := make([]byte, 4)
	n := s.Len()
	binary.BigEndian.PutUint32(int32Arr, uint32(n))
	_, err := buf.Write(int32Arr)
	if err != nil {
		return nil, err
	}


	addrHashList,nonDefaultIncarnationsList, addrMap:=createDict3(s)

	//spew.Dump(addrMap)

	lenOfAddHashes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenOfAddHashes, uint16(len(addrMap)))
	_, err = buf.Write(lenOfAddHashes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(addrHashList)
	if err != nil {
		return nil, err
	}

	fmt.Println("storageKeys", 4+2+len(addrHashList))
	storageKeys, valuesBytes, lengthOfValuesBytes:=computeStorageKeys3(s,addrMap)
	_, err = buf.Write(storageKeys)
	if err != nil {
		return nil, err
	}

	fmt.Println("lengthOfValuesBytes",  4+2+len(addrHashList)+len(storageKeys))
	_, err = buf.Write(lengthOfValuesBytes)
	if err != nil {
		return nil, err
	}

	fmt.Println("valuesBytes",  4+2+len(addrHashList)+len(storageKeys)+len(lengthOfValuesBytes))
	_, err = buf.Write(valuesBytes)
	if err != nil {
		return nil, err
	}

	fmt.Println("nonDefaultIncarnationsList",  4+2+len(addrHashList)+len(storageKeys)+len(lengthOfValuesBytes)+len(valuesBytes))
	if len(nonDefaultIncarnationsList)>0 {
		fmt.Println("nonDefaultIncarnationsList", len(nonDefaultIncarnationsList))
	}
	_, err = buf.Write(nonDefaultIncarnationsList)
	if err != nil {
		return nil, err
	}


	byt := buf.Bytes()
	return byt, nil
}




func createDict3(s *ChangeSet) ([]byte, []byte, map[common.Hash]uint16) {
	n := s.Len()
	addrHashesMap := make(map[common.Hash]uint16, 0)
	addrHashList := make([]byte,0)
	notDefaultIncarnationList := make([]byte,0)


	nextIDAddrHash :=uint16(0)

	for i := 0; i < n; i++ {
		//copy addrHash
		addrHash:=common.Hash{}
		copy(
			addrHash[:],
			s.Changes[i].Key[0:common.HashLength],
		)


		if _, ok:= addrHashesMap[addrHash]; !ok {
			addrHashesMap[addrHash]=nextIDAddrHash
			nextIDAddrHash++
			addrHashList=append(addrHashList, addrHash.Bytes()...)
		}


		incarnation := binary.BigEndian.Uint64(s.Changes[i].Key[common.HashLength : common.HashLength+common.IncarnationLength])
		if incarnation != DefaultIncarnation {
			inc:=make([]byte, 12)
			binary.BigEndian.PutUint32(inc[0:4],uint32(i))
			binary.BigEndian.PutUint64(inc[4:12],incarnation)
			notDefaultIncarnationList=append(notDefaultIncarnationList, inc...)
		}
	}

	return addrHashList, notDefaultIncarnationList, addrHashesMap
}



func computeStorageKeys3(s *ChangeSet, addrHashes map[common.Hash]uint16) ([]byte,[]byte,[]byte) {
	lenOfAddr:=getNumOfBytesByLen(len(addrHashes))
	values:=new(bytes.Buffer)
	lengthes:=make([]byte, 2+2+2)
	numOfUint8:=uint16(0)
	numOfUint16:=uint16(0)
	numOfUint32:=uint16(0)

	keys :=new(bytes.Buffer)
	lengthOfValues:=uint32(0)
	for i:=0; i<len(s.Changes);i++ {
		row:=make([]byte, lenOfAddr+common.HashLength)
		write(
			addrHashes[common.BytesToHash(s.Changes[i].Key[0:common.HashLength])],
			row[0:lenOfAddr],
		)
		copy(row[lenOfAddr:lenOfAddr+common.HashLength], common.CopyBytes(s.Changes[i].Key[common.IncarnationLength+common.HashLength:common.IncarnationLength+2*common.HashLength]))
		keys.Write(row)

		lengthOfValues+=uint32(len(s.Changes[i].Value))
		switch {
		case lengthOfValues<=255:
			fmt.Println("8", numOfUint8, lengthOfValues)
			numOfUint8++
			lengthes=append(lengthes, uint8(lengthOfValues))
		case lengthOfValues<=65535:
			fmt.Println("16",numOfUint16, lengthOfValues)
			numOfUint16++
			uint16b:=make([]byte, 2)
			binary.BigEndian.PutUint16(uint16b, uint16(lengthOfValues))
			lengthes=append(lengthes, uint16b...)
		default:
			numOfUint32++
			uint32b:=make([]byte, 4)
			binary.BigEndian.PutUint32(uint32b, lengthOfValues)
			lengthes=append(lengthes, uint32b...)
		}
		values.Write(s.Changes[i].Value)
	}

	//fmt.Println("numOfUint8", numOfUint8, numOfUint16, numOfUint32)
	binary.BigEndian.PutUint16(lengthes[0:2], numOfUint8)
	binary.BigEndian.PutUint16(lengthes[2:4], numOfUint16)
	binary.BigEndian.PutUint16(lengthes[4:6], numOfUint32)
	return keys.Bytes(), values.Bytes(), lengthes

}




func DecodeStorageDict3(b []byte) (*ChangeSet, error) {
	fmt.Println()
	fmt.Println("decode")
	h := NewStorageChangeSet()
	if len(b) == 0 {
		h.Changes = make([]Change, 0)
		return h, nil
	}
	if len(b) < 4 {
		return h, fmt.Errorf("decode: input too short (%d bytes)", len(b))
	}

	//numOfElements uint32
	numOfElements := binary.BigEndian.Uint32(b[0:4])
	h.Changes = make([]Change, numOfElements)

	if numOfElements == 0 {
		return h, nil
	}

	dictLen:=binary.BigEndian.Uint16(b[4:6])
	addMap:=make(map[uint16]common.Hash)
	for i:=0; i< int(dictLen); i++ {
		addMap[uint16(i)] = common.BytesToHash(b[6+i*common.HashLength:6+i*common.HashLength+common.HashLength])
	}


	lenOfValsPos:=storageEnodingStartElem+
		2 + uint32(dictLen)*common.HashLength+
		numOfElements*uint32(getNumOfBytesByLen(int(dictLen))+common.HashLength)
	numOfUint8:=int(binary.BigEndian.Uint16(b[lenOfValsPos:lenOfValsPos+2]))
	numOfUint16:=int(binary.BigEndian.Uint16(b[lenOfValsPos+2:lenOfValsPos+4]))
	numOfUint32:=int(binary.BigEndian.Uint16(b[lenOfValsPos+4:lenOfValsPos+6]))
	fmt.Println("lengthOfValuesBytes",b[lenOfValsPos:lenOfValsPos+6])
	fmt.Println("lengthOfValuesBytes", lenOfValsPos)
	lenOfValsPos=lenOfValsPos+6
	valuesPos:=lenOfValsPos+uint32(numOfUint8) + uint32(numOfUint16*2) + uint32(numOfUint32*4)
	fmt.Println("valuesPos", valuesPos)

	//parse not default incarnations

	//????????
	fmt.Println(numOfUint8, numOfUint16, numOfUint32)
	incarnationPosition:=lenOfValsPos+uint32(calculateIncarnationPos3(b[lenOfValsPos:], numOfUint8, numOfUint16, numOfUint32))
	fmt.Println("incarnationPosition", incarnationPosition)
	incarnationsLength := len(b[incarnationPosition:])
	notDefaultIncarnation := make(map[uint32]uint64, 0)
	var (
		id  uint32
		inc uint64
		ok  bool
	)

	if incarnationsLength > 0 {
		if incarnationsLength%(4+common.IncarnationLength) != 0 {
			return h, fmt.Errorf("decode: incarnatin part is incorrect(%d bytes)", len(b[incarnationPosition:]))
		}
		numOfIncarnations := incarnationsLength / (4 + common.IncarnationLength)
		for i := 0; i < numOfIncarnations; i++ {
			id = binary.BigEndian.Uint32(b[incarnationPosition : incarnationPosition+4])
			inc = binary.BigEndian.Uint64(b[incarnationPosition+4 : incarnationPosition+4+8])
			notDefaultIncarnation[id] = inc
			incarnationPosition += (4 + common.IncarnationLength)
		}
	}

	elementStart := storageEnodingStartElem + 2+uint32(dictLen)*common.HashLength
	key := make([]byte, common.HashLength*2+common.IncarnationLength)

	lenOfAddHash:=uint32(getNumOfBytesByLen(len(addMap)))
	//lastValLen:=0
	for i := uint32(0); i < numOfElements; i++ {
		//copy addrHash
		elem:=elementStart+i*(lenOfAddHash+common.HashLength)
		copy(
			key[0:common.HashLength],
			readFromMap(addMap, b[elem:elem+lenOfAddHash]).Bytes(),
		)
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			common.CopyBytes(b[elem+lenOfAddHash:elem+lenOfAddHash+common.HashLength]),
		)
		//set incarnation
		if inc, ok = notDefaultIncarnation[i]; ok {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], inc)
		} else {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], DefaultIncarnation)
		}

		h.Changes[i].Key = common.CopyBytes(key)
		h.Changes[i].Value =findVal(b[lenOfValsPos:valuesPos], b[valuesPos:], i, numOfUint8, numOfUint16, numOfUint32)
	}
	return h, nil
}


func calculateIncarnationPos3(b []byte, numOfUint8, numOfUint16, numOfUint32 int) int {
	res :=0
	end:=0
	switch {
	case numOfUint32>0:
		fmt.Println("32")
		end=numOfUint8+numOfUint16*2+numOfUint32*4
		res= int(binary.BigEndian.Uint32(b[end-4:end])) + end
	case numOfUint16>0:
		fmt.Println("16")
		end=numOfUint8+numOfUint16*2
		res= int(binary.BigEndian.Uint16(b[end-2:end])) +end
	case numOfUint8>0:
		fmt.Println("8")
		end=numOfUint8
		res= int(b[end-1]) + end
	default:
		return 0
	}
	fmt.Println("numOfUint16",numOfUint16)
	fmt.Println("end", end)
	fmt.Println("res", res)
	return res
}