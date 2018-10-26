package io

import (
	"encoding/binary"
	"errors"
	"github.com/xosmig/extsort/util"
	"io"
)

type Uint64Writer interface {
	WriteUint64(x uint64) error
	Flush() error
}

type BinaryUint64Writer struct {
	stream    io.Writer
	valuesBuf []uint64
	writeBuf  []byte
}

var ErrTooSmallBuffer = errors.New("too small buffer provided")

func NewBinaryUint64WriterCountBuf(w io.Writer, count int, bytesBuf []byte) *BinaryUint64Writer {
	if len(bytesBuf) < count*SizeOfValue {
		panic(ErrTooSmallBuffer)
	}

	return &BinaryUint64Writer{
		stream:    w,
		valuesBuf: make([]uint64, 0, count),
		writeBuf:  bytesBuf,
	}
}

func NewBinaryUint64WriterCount(w io.Writer, count int) *BinaryUint64Writer {
	return NewBinaryUint64WriterCountBuf(w, count, NewUint64ByteBuf(count))
}

func NewBinaryUint64Writer(w io.Writer) *BinaryUint64Writer {
	return NewBinaryUint64WriterCount(w, DefaultBufValuesCount)
}

func (w *BinaryUint64Writer) Flush() error {
	count := len(w.valuesBuf)
	for valueIdx := 0; valueIdx < count; valueIdx++ {
		binary.LittleEndian.PutUint64(w.writeBuf[valueIdx*SizeOfValue:], w.valuesBuf[valueIdx])
	}
	_, err := w.stream.Write(w.writeBuf[:count*SizeOfValue])
	if err != nil {
		return err
	}

	w.valuesBuf = w.valuesBuf[:0]
	return nil
}

func (w *BinaryUint64Writer) WriteUint64(x uint64) error {
	if len(w.valuesBuf) == cap(w.valuesBuf) {
		err := w.Flush()
		if err != nil {
			return err
		}
	}

	w.valuesBuf = append(w.valuesBuf, x)
	return nil
}

type SliceUint64Writer struct{ data []uint64 }

func NewSliceUint64Writer() *SliceUint64Writer {
	return new(SliceUint64Writer)
}

func (w *SliceUint64Writer) Flush() error {
	return nil
}

func (w *SliceUint64Writer) WriteUint64(x uint64) error {
	w.data = append(w.data, x)
	return nil
}

func (w *SliceUint64Writer) Data() []uint64 {
	_, _ = util.NewSharedBufHeap(100)
	return w.data
}

type NullUint64Writer struct{}

func NewNullUint64Writer() NullUint64Writer {
	return NullUint64Writer{}
}

func (w NullUint64Writer) Flush() error {
	return nil
}

func (w NullUint64Writer) WriteUint64(x uint64) error {
	return nil
}
