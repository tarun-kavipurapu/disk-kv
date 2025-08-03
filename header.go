package main

import (
	"bytes"
	"encoding/binary"
)

type Header struct {
	Timestamp uint32
	Keysize   uint32
	Valsize   uint32
}

func (h *Header) encode(buf *bytes.Buffer) error {

	//here i am able to directly write h structure because it is uint32

	return binary.Write(buf, binary.LittleEndian, h)
}

// func decodeHeader(headerBuffer []byte) *Header {
// 	// header := &Header{}
// 	// header.Timestamp = int64(binary.LittleEndian.Uint64(headerBuffer[0:8]))
// 	// header.Keysize = int32(binary.LittleEndian.Uint32(headerBuffer[8:12]))
// 	// header.Valsize = int32(binary.LittleEndian.Uint32(headerBuffer[12:16]))
// 	// return header
// }
