package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	defaultBufSize = 4096
	sizeOfValue    = 8
)

type Uint64Reader interface {
	ReadUint64() (uint64, error)
}

type BinaryUint64Reader struct {
	stream     io.Reader
	valuesBuf  []uint64
	valuesTail []uint64
	memBuf     []byte
}

func NewBinaryUint64ReaderSize(r io.Reader, size int) *BinaryUint64Reader {
	valuesBuf := make([]uint64, size)
	return &BinaryUint64Reader{
		stream:     r,
		valuesBuf:  valuesBuf,
		valuesTail: valuesBuf[:0],
		memBuf:     make([]byte, size*sizeOfValue),
	}
}

func NewBinaryUint64Reader(r io.Reader) *BinaryUint64Reader {
	return NewBinaryUint64ReaderSize(r, defaultBufSize)
}

// either puts 1 or more new values to valuesTail or returns an error
func (r *BinaryUint64Reader) fillEmpty() error {
	// TODO: insert profiling here
	n, err := io.ReadFull(r.stream, r.memBuf)
	// TODO: insert profiling here

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = nil
		if n == 0 {
			return io.EOF
		}
	}

	if err != nil {
		return err
	}

	if n%sizeOfValue != 0 {
		return fmt.Errorf("expected to read number of bytes divisible by %v, read %v bytes", sizeOfValue, n)
	}

	valueIdx := 0
	for memTail := r.memBuf[:n]; len(memTail) > 0; memTail = memTail[sizeOfValue:] {
		r.valuesBuf[valueIdx] = binary.LittleEndian.Uint64(memTail)
		valueIdx++
	}

	r.valuesTail = r.valuesBuf[:valueIdx]
	return nil
}

func (r *BinaryUint64Reader) ReadUint64() (uint64, error) {
	if len(r.valuesTail) == 0 {
		err := r.fillEmpty()
		if err != nil {
			return 0, err
		}
	}

	result := r.valuesTail[0]
	r.valuesTail = r.valuesTail[1:]
	return result, nil
}
