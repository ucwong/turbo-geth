package changeset

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/davecgh/go-spew/spew"
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
	binary.BigEndian.PutUint32(intArr, uint32(n))
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
		binary.BigEndian.PutUint16(storageKey[0:lenOfStorageIndex],idAddr)
		binary.BigEndian.PutUint16(storageKey[lenOfStorageIndex:2*lenOfStorageIndex],idKey)
		storageKeys=append(storageKeys, storageKey)

		incarnation := binary.BigEndian.Uint64(s.Changes[i].Key[common.HashLength : common.HashLength+common.IncarnationLength])
		if incarnation != DefaultIncarnation {
			notDefaultIncarnationList = append(notDefaultIncarnationList, struct {
				ID  uint8
				Inc uint64
			}{ID: uint8(i), Inc: incarnation})
		}
	}

	lenOfAddHashes := make([]byte, 2)
	binary.BigEndian.PutUint16(lenOfAddHashes, uint16(len(addrHashList)))
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

	binary.BigEndian.PutUint16(lenOfAddHashes, uint16(len(keyHashesList)))
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
	binary.BigEndian.PutUint32(bb, lenOfVals)
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


func DecodeStorageDict(b []byte) (*ChangeSet, error) {
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

	dictLen:=binary.BigEndian.Uint16(b[4:6])
	addMap:=make(map[uint16]common.Hash)
	for i:=0; i< int(dictLen); i++ {
		addMap[uint16(i)] = common.BytesToHash(b[6+i*common.HashLength:6+i*common.HashLength+common.HashLength])
	}

	dictKeysStart:=6+dictLen*common.HashLength
	dictKeyLen:=binary.BigEndian.Uint16(b[dictKeysStart:dictKeysStart+2])

	keyMap:=make(map[uint16]common.Hash)
	for i:=0; i< int(dictKeyLen); i++ {
		keyMap[uint16(i)] = common.BytesToHash(b[int(dictKeysStart)+2+i*common.HashLength:int(dictKeysStart)+2+i*common.HashLength+common.HashLength])
	}

	lenOfValsPos:=storageEnodingStartElem + 2+uint32(dictLen)*common.HashLength + 2+ uint32(dictKeyLen)*common.HashLength + numOfElements*4
	lenOfVals := binary.BigEndian.Uint32(b[lenOfValsPos:lenOfValsPos+4])

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
			id = binary.BigEndian.Uint32(b[incarnationPosition : incarnationPosition+4])
			inc = binary.BigEndian.Uint64(b[incarnationPosition+4 : incarnationPosition+4+8])
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
			addMap[binary.BigEndian.Uint16(b[elem:elem+2])].Bytes(),
			)
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			keyMap[binary.BigEndian.Uint16(b[elem+2:elem+4])].Bytes(),
		)
		//set incarnation
		if inc, ok = notDefaultIncarnation[i]; ok {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], inc)
		} else {
			binary.BigEndian.PutUint64(key[common.HashLength:common.HashLength+common.IncarnationLength], DefaultIncarnation)
		}
		valLen:=b[lenOfValsPos+4+uint32(i)]


		h.Changes[i].Key = common.CopyBytes(key)
		h.Changes[i].Value =common.CopyBytes(b[valPos:valPos+uint32(valLen)])
		valPos+=uint32(valLen)
	}

	return h, nil
}


/*
Storage ChangeSet is serialized in the following manner in order to facilitate binary search:

numOfElements uint32
numOfUniqAddrHashes uint32
numOfUniqKeys uint32
[addrHashes] []common.Hash
[keys] []common.Hash
[idOfAddr:idOfKey] [uint8/uint16/uint32:uint8/uint16/uint32...] (depends on numOfUniqAddrHashes, numOfUniqKeys)
numOfUint8Values uint8
numOfUint16Values uint16
numOfUint32Values uint16
[len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint8Values-1})] []uint8
[len(valnumOfUint8Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint16Values-1})] []uint16
[len(valnumOfUint16Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint32Values-1})] []uint32
[elementNum:incarnation] -  optional [uint32:uint64...]

*/


func EncodeStorageDict2(s *ChangeSet) ([]byte, error) {
	sort.Sort(s)
	buf := new(bytes.Buffer)
	int32Arr := make([]byte, 4)
	n := s.Len()
	binary.BigEndian.PutUint32(int32Arr, uint32(n))
	_, err := buf.Write(int32Arr)
	if err != nil {
		return nil, err
	}


	addrHashList,keyHashesList,nonDefaultIncarnationsList, addrMap,keyMap:=createDict(s)

	spew.Dump(addrMap)
	spew.Dump(keyMap)

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

	binary.BigEndian.PutUint16(lenOfAddHashes, uint16(len(keyMap)))
	_, err = buf.Write(lenOfAddHashes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(keyHashesList)
	if err != nil {
		return nil, err
	}


	storageKeys, valuesBytes, lengthOfValuesBytes:=computeStorageKeys(s,addrMap, keyMap)
	_, err = buf.Write(storageKeys)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(lengthOfValuesBytes)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(valuesBytes)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(nonDefaultIncarnationsList)
	if err != nil {
		return nil, err
	}


	byt := buf.Bytes()
	return byt, nil
}

func computeStorageKeys(s *ChangeSet, addrHashes map[common.Hash]uint16, keyHashes map[common.Hash]uint16) ([]byte,[]byte,[]byte) {
	lenOfAddr:=getNumOfBytesByLen(len(addrHashes))
	lenOfkeys:=getNumOfBytesByLen(len(keyHashes))
	values:=new(bytes.Buffer)
	lengthes:=make([]byte, 1+2+2)
	numOfUint8:=uint8(0)
	numOfUint16:=uint16(0)
	numOfUint32:=uint16(0)

	keys :=new(bytes.Buffer)
	lengthOfValues:=uint32(0)
	for i:=0; i<len(s.Changes);i++ {
		row:=make([]byte, lenOfAddr+lenOfkeys)
		write(
			addrHashes[common.BytesToHash(s.Changes[i].Key[0:common.HashLength])],
			row[0:lenOfAddr],
			)
		write(
			keyHashes[common.BytesToHash(s.Changes[i].Key[common.IncarnationLength+common.HashLength:common.IncarnationLength+2*common.HashLength])],
			row[lenOfAddr:lenOfAddr+lenOfkeys],
			)

		keys.Write(row)

		lengthOfValues+=uint32(len(s.Changes[i].Value))
		switch {
		case lengthOfValues<255:
			numOfUint8++
			lengthes=append(lengthes, uint8(lengthOfValues))
		case lengthOfValues<65535:
			numOfUint16++
			uint16b:=make([]byte, 2)
			binary.BigEndian.PutUint16(uint16b, uint16(lengthOfValues))
			lengthes=append(lengthes, uint16b...)
		default:
			numOfUint32++
			uint32b:=make([]byte, 4)
			binary.BigEndian.PutUint32(uint32b, uint32(lengthOfValues))
			lengthes=append(lengthes, uint32b...)

		}
		values.Write(s.Changes[i].Value)
	}

	lengthes[0]=numOfUint8
	binary.BigEndian.PutUint16(lengthes[1:3], numOfUint16)
	binary.BigEndian.PutUint16(lengthes[3:5], numOfUint32)
	return keys.Bytes(), values.Bytes(), lengthes

}
func getNumOfBytesByLen(n int) int {
	switch {
	case n<255:
		return 1
	case n<65535:
		return 2
	case n<4294967295:
		return 4
	default:
		return 8
	}
}
func write(id uint16 , row []byte)  {
	switch len(row) {
	case 1:
		row[0]=uint8(id)
	case 2:
		binary.BigEndian.PutUint16(row, id)
	case 4:
		binary.BigEndian.PutUint32(row, uint32(id))
	case 8:
		binary.BigEndian.PutUint64(row, uint64(id))
	default:
		panic("wrong")
	}
	return

}
func readFromMap(m map[uint16]common.Hash , row []byte) common.Hash {
	switch len(row) {
	case 1:
		return m[uint16(row[0])]
	case 2:
		return m[binary.BigEndian.Uint16(row)]
	case 4:
		return m[uint16(binary.BigEndian.Uint32(row))]
	case 8:
		return m[uint16(binary.BigEndian.Uint64(row))]
	default:
		panic("wrong")
	}
}

func createDict(s *ChangeSet) ([]byte,[]byte,[]byte, map[common.Hash]uint16, map[common.Hash]uint16) {
	n := s.Len()
	addrHashesMap := make(map[common.Hash]uint16, 0)
	addrHashList := make([]byte,0)
	keyHashesMap := make(map[common.Hash]uint16, 0)
	keyHashesList := make([]byte,0)
	notDefaultIncarnationList := make([]byte,0)


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


		if _, ok:= addrHashesMap[addrHash]; !ok {
			addrHashesMap[addrHash]=nextIDAddrHash
			nextIDAddrHash++
			addrHashList=append(addrHashList, addrHash.Bytes()...)
		}

		//copy key
		copy(
			keyHash[:],
			s.Changes[i].Key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
		)


		if _, ok:= keyHashesMap[addrHash]; !ok {
			keyHashesMap[keyHash]=nextIDKeyHash
			nextIDKeyHash++
			keyHashesList=append(keyHashesList, keyHash.Bytes()...)
		}


		incarnation := binary.BigEndian.Uint64(s.Changes[i].Key[common.HashLength : common.HashLength+common.IncarnationLength])
		if incarnation != DefaultIncarnation {
			inc:=make([]byte, 10)
			binary.BigEndian.PutUint16(inc[0:2],uint16(i))
			binary.BigEndian.PutUint64(inc[2:10],incarnation)
			notDefaultIncarnationList=append(notDefaultIncarnationList, inc...)
		}
	}

	return addrHashList, keyHashesList, notDefaultIncarnationList, addrHashesMap, keyHashesMap
}



/*
numOfElements uint32
numOfUniqAddrHashes uint16
numOfUniqKeys uint16
[addrHashes] []common.Hash
[keys] []common.Hash
[idOfAddr:idOfKey] [uint8/uint16/uint32:uint8/uint16/uint32...] (depends on numOfUniqAddrHashes, numOfUniqKeys)
numOfUint8Values uint8
numOfUint16Values uint16
numOfUint32Values uint16
[len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint8Values-1})] []uint8
[len(valnumOfUint8Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint16Values-1})] []uint16
[len(valnumOfUint16Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint32Values-1})] []uint32
[elementNum:incarnation] -  optional [uint32:uint64...]

 */
func DecodeStorageDict2(b []byte) (*ChangeSet, error) {
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
	fmt.Println(dictLen)
	for i:=0; i< int(dictLen); i++ {
		fmt.Println(i)
		addMap[uint16(i)] = common.BytesToHash(b[6+i*common.HashLength:6+i*common.HashLength+common.HashLength])
	}

	dictKeysStart:=6+dictLen*common.HashLength
	dictKeyLen:=binary.BigEndian.Uint16(b[dictKeysStart:dictKeysStart+2])

	keyMap:=make(map[uint16]common.Hash)
	for i:=0; i< int(dictKeyLen); i++ {
		keyMap[uint16(i)] = common.BytesToHash(b[int(dictKeysStart)+2+i*common.HashLength:int(dictKeysStart)+2+i*common.HashLength+common.HashLength])
	}
	spew.Dump(addMap)
	spew.Dump(keyMap)


	lenOfValsPos:=storageEnodingStartElem + 2+uint32(dictLen)*common.HashLength + 2+ uint32(dictKeyLen)*common.HashLength + numOfElements*uint32(getNumOfBytesByLen(int(dictLen)))+numOfElements*uint32(getNumOfBytesByLen(int(dictKeyLen)))
	numOfUint8:=int(b[lenOfValsPos])
	numOfUint16:=int(binary.BigEndian.Uint16(b[lenOfValsPos+1:lenOfValsPos+3]))
	numOfUint32:=int(binary.BigEndian.Uint16(b[lenOfValsPos+3:lenOfValsPos+5]))
	fmt.Println(numOfUint8, numOfUint16, numOfUint32)
	valuesPos:=lenOfValsPos+1+2+2+uint32(numOfUint8) + uint32(numOfUint16*2) + uint32(numOfUint32*4)
	lenOfValsPos=lenOfValsPos+5
	//numOfUniqAddrHashes uint16 +
	//[addrHashes] []common.Hash +
	//numOfUniqKeys uint16 +
	//[keys] []common.Hash +
	//[idOfAddr:idOfKey] [uint8/uint16/uint32:uint8/uint16/uint32...] (depends on numOfUniqAddrHashes, numOfUniqKeys) +
	//numOfUint8Values uint8 +
	//numOfUint16Values uint16 +
	//numOfUint32Values uint16 +
	//[len(val0), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint8Values-1})] []uint8
	//[len(valnumOfUint8Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint16Values-1})] []uint16
	//[len(valnumOfUint16Values), len(val0)+len(val1), ..., len(val0)+len(val1)+...+len(val_{numOfUint32Values-1})] []uint32
	//[elementNum:incarnation] -  optional [uint32:uint64...]


	//lenOfVals := binary.BigEndian.Uint32(b[lenOfValsPos:lenOfValsPos+4])

	//incarnationPosition := lenOfValsPos+4+lenOfVals
	//if uint32(len(b)) < incarnationPosition {
	//	return nil, fmt.Errorf("decode: input too short (%d bytes, expected at least %d bytes)", len(b), incarnationPosition)
	//}

	//parse not default incarnations

	//????????
	incarnationPosition:=lenOfValsPos+uint32(calculateIncarnationPos(b[lenOfValsPos:], numOfUint8, numOfUint16, numOfUint32))
	incarnationsLength := len(b[incarnationPosition:])
	notDefaultIncarnation := make(map[uint32]uint64, 0)
	var (
		id  uint32
		inc uint64
		ok  bool
	)

	if incarnationsLength > 0 {
		if incarnationsLength%(2+common.IncarnationLength) != 0 {
			fmt.Println("+inc")
			fmt.Println(b[incarnationPosition:])
			fmt.Println("-inc")
			return h, fmt.Errorf("decode: incarnatin part is incorrect(%d bytes)", len(b[incarnationPosition:]))
		}
		numOfIncarnations := incarnationsLength / (2 + common.IncarnationLength)
		for i := 0; i < numOfIncarnations; i++ {
			id = binary.BigEndian.Uint32(b[incarnationPosition : incarnationPosition+2])
			inc = binary.BigEndian.Uint64(b[incarnationPosition+2 : incarnationPosition+2+8])
			notDefaultIncarnation[id] = inc
			incarnationPosition += (2 + common.IncarnationLength)
		}
	}

	elementStart := storageEnodingStartElem + 2+uint32(dictLen)*common.HashLength + 2+ uint32(dictKeyLen)*common.HashLength
	key := make([]byte, common.HashLength*2+common.IncarnationLength)

	lenOfAddHash:=uint32(getNumOfBytesByLen(len(addMap)))
	lenOfKey:=uint32(getNumOfBytesByLen(len(keyMap)))
	//lastValLen:=0
	for i := uint32(0); i < numOfElements; i++ {
		//copy addrHash
		elem:=elementStart+i*(lenOfAddHash+lenOfKey)
		copy(
			key[0:common.HashLength],
			readFromMap(addMap, b[elem:elem+lenOfAddHash]).Bytes(),
		)
		//copy key hash
		copy(
			key[common.HashLength+common.IncarnationLength:2*common.HashLength+common.IncarnationLength],
			readFromMap(keyMap, b[elem+lenOfAddHash:elem+lenOfAddHash+lenOfKey]).Bytes(),
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

func findVal(lenOfVals []byte, values []byte, i uint32,  numOfUint8, numOfUint16, numOfUint32 int) []byte {
	//fmt.Println(i)
	lenOfValStart:=uint32(0)
	lenOfValEnd:=uint32(0)
	switch {
	case i < uint32(numOfUint8):
		lenOfValEnd=uint32(lenOfVals[i])
		if i>0 {
			lenOfValStart=uint32(lenOfVals[i-1])
		}
		return common.CopyBytes(values[lenOfValStart:lenOfValEnd])
	case i < uint32(numOfUint8)+uint32(numOfUint16):
		one:=uint32(numOfUint8)+(i-uint32(numOfUint8))*2
		lenOfValEnd=uint32(binary.BigEndian.Uint16(lenOfVals[one:one+2]))
		if i-1<uint32(numOfUint8) {
			lenOfValStart=uint32(lenOfVals[i-1])
		} else {
			one=uint32(numOfUint8)+(i-1-uint32(numOfUint8))*2
			lenOfValStart=uint32(binary.BigEndian.Uint16(lenOfVals[one:one+2]))
		}
		return common.CopyBytes(values[lenOfValStart:lenOfValEnd])
	case i < uint32(numOfUint8)+uint32(numOfUint16)+uint32(numOfUint32):
		one:=uint32(numOfUint8)+uint32(numOfUint16)*2+i-uint32(numOfUint8)-uint32(numOfUint16)*4
		lenOfValEnd=uint32(binary.BigEndian.Uint32(lenOfVals[one:one+4]))
		if i-1<uint32(numOfUint8) + uint32(numOfUint16) {
			one=uint32(numOfUint8)+(i-1-uint32(numOfUint8))*2
			lenOfValStart=uint32(binary.BigEndian.Uint16(lenOfVals[one:one+2]))
		} else {
			one=uint32(numOfUint8)+(i-1-uint32(numOfUint8))*2
			lenOfValStart=uint32(binary.BigEndian.Uint32(lenOfVals[one:one+4]))
		}
		return common.CopyBytes(values[lenOfValStart:lenOfValEnd])
	default:
		panic("findval err")
	}
}

func calculateIncarnationPos(b []byte, numOfUint8, numOfUint16, numOfUint32 int) int {
	//fmt.Println(b)
	res :=0
	end:=0
	switch {
	case numOfUint32>0:
		end=numOfUint8+numOfUint16*2+numOfUint32*4
		res= int(binary.BigEndian.Uint32(b[end-4:end]))
	case numOfUint16>0:
		end=numOfUint8+numOfUint16*2
		res= int(binary.BigEndian.Uint16(b[end-2:end]))
	case numOfUint8>0:
		end=numOfUint8
		res= int(b[end-1])
	default:
		return 0
	}
	//fmt.Println("calculateIncarnationPos", end)
	//fmt.Println("calculateIncarnationPos", res)
	//fmt.Println("calculateIncarnationPos", b[res])
	return res
}