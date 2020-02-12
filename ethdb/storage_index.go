package ethdb

import (
	"bytes"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ugorji/go/codec"
	"io"
	"log"
)

func NewStorageIndex() StorageIndex {
	return make(StorageIndex)
}

type StorageIndex map[common.Hash]*HistoryIndex

func (si StorageIndex) Encode() ([]byte, error) {
	w:=bytes.NewBuffer([]byte{})
	encoder := Encoder(w)
	defer Return(encoder)
	err := encoder.Encode(si)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (si StorageIndex) Decode(s []byte) error {
	if len(s) == 0 {
		return nil
	}

	decoder := Decoder(bytes.NewReader(s))
	defer Return(decoder)

	return decoder.Decode(&si)
}

func (si StorageIndex) Append(key common.Hash, val uint64) {
	if _, ok := si[key]; !ok {
		si[key] = new(HistoryIndex)
	}
	si[key] = si[key].Append(val)
}

//most common operation is remove one from the tail
func (si StorageIndex) Remove(key common.Hash, val uint64) {
	if v, ok := si[key]; ok && v != nil {
		v = v.Remove(val)
		if len(*v) == 0 {
			delete(si, key)
		} else {
			si[key] = v
		}
	}
}

func (si StorageIndex) Search(key common.Hash, val uint64) (uint64, bool) {
	if v, ok := si[key]; ok && v != nil {
		return v.Search(val)
	}
	return 0, false
}

func AppendToStorageIndex(b []byte, key []byte, timestamp uint64) ([]byte, error) {
	v := NewStorageIndex()

	if err := v.Decode(b); err != nil {
		return nil, err
	}

	v.Append(common.BytesToHash(key), timestamp)
	return v.Encode()
}
func RemoveFromStorageIndex(b []byte, timestamp uint64) ([]byte, bool, error) {
	v := NewStorageIndex()

	if err := v.Decode(b); err != nil {
		return nil, false, err
	}

	for key := range v {
		v.Remove(key, timestamp)
	}

	res, err := v.Encode()
	if len(v) == 0 {
		return res, true, err
	}
	return res, false, err
}



// Pool of decoders
var decoderPool = make(chan *codec.Decoder, 128)

func Decoder(r io.Reader) *codec.Decoder {
	var d *codec.Decoder
	select {
	case d = <-decoderPool:
		d.Reset(r)
	default:
		{
			var handle codec.CborHandle
			handle.ReaderBufferSize = 64 * 1024
			d = codec.NewDecoder(r, &handle)
		}
	}
	return d
}

func returnDecoderToPool(d *codec.Decoder) {
	select {
	case decoderPool <- d:
	default:
		log.Println("Allowing decoder to be garbage collected, pool is full")
	}
}

// Pool of encoders
var encoderPool = make(chan *codec.Encoder, 128)

func Encoder(w io.Writer) *codec.Encoder {
	var e *codec.Encoder
	select {
	case e = <-encoderPool:
		e.Reset(w)
	default:
		{
			var handle codec.CborHandle
			handle.WriterBufferSize = 64 * 1024
			e = codec.NewEncoder(w, &handle)
		}
	}
	return e
}

func returnEncoderToPool(e *codec.Encoder) {
	select {
	case encoderPool <- e:
	default:
		log.Print("Allowing encoder to be garbage collected, pool is full")
	}
}

func Return(d interface{}) {
	switch toReturn := d.(type) {
	case *codec.Decoder:
		returnDecoderToPool(toReturn)
	case *codec.Encoder:
		returnEncoderToPool(toReturn)
	default:
		panic(fmt.Sprintf("unexpected type: %T", d))
	}
}