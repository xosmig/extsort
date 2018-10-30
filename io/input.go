package io

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/xosmig/extsort/util"
	"io"
)

type Uint64Reader interface {
	ReadUint64() (uint64, error)
	SetProfiler(p *util.SimpleProfiler)
}

type BinaryUint64Reader struct {
	stream     io.Reader
	valuesBuf  []uint64
	valuesTail []uint64
	readBuf    []byte
	profiler   *util.SimpleProfiler
}

func NewBinaryUint64ReaderCountBuf(r io.Reader, count int, bytesBuf []byte) *BinaryUint64Reader {
	if len(bytesBuf) < count*SizeOfValue {
		panic(ErrTooSmallBuffer)
	}

	valuesBuf := make([]uint64, count)
	return &BinaryUint64Reader{
		stream:     r,
		valuesBuf:  valuesBuf,
		valuesTail: valuesBuf[:0],
		readBuf:    bytesBuf,
		profiler:   util.NewNilSimpleProfiler(),
	}
}

func NewBinaryUint64ReaderCount(r io.Reader, count int) *BinaryUint64Reader {
	return NewBinaryUint64ReaderCountBuf(r, count, NewUint64ByteBuf(count))
}

func NewBinaryUint64Reader(r io.Reader) *BinaryUint64Reader {
	return NewBinaryUint64ReaderCount(r, DefaultBufValuesCount)
}

func (r *BinaryUint64Reader) SetProfiler(p *util.SimpleProfiler) {
	r.profiler = p
}

// either puts 1 or more new values to valuesTail or returns an error
func (r *BinaryUint64Reader) fillEmpty() error {
	r.profiler.StartMeasuring()
	n, err := io.ReadFull(r.stream, r.readBuf)
	r.profiler.FinishMeasuring()

	if err == io.EOF || err == io.ErrUnexpectedEOF {
		err = nil
		if n == 0 {
			return io.EOF
		}
	}

	if err != nil {
		return err
	}

	if n%SizeOfValue != 0 {
		return fmt.Errorf("expected to read number of bytes divisible by %v, read %v bytes", SizeOfValue, n)
	}

	count := n / SizeOfValue
	for valueIdx := 0; valueIdx < count; valueIdx++ {
		r.valuesBuf[valueIdx] = binary.LittleEndian.Uint64(r.readBuf[valueIdx*SizeOfValue:])
	}

	r.valuesTail = r.valuesBuf[:count]
	return nil
}

func (r *BinaryUint64Reader) PeekUint64() (uint64, error) {
	if len(r.valuesTail) == 0 {
		err := r.fillEmpty()
		if err != nil {
			return 0, err
		}
	}

	return r.valuesTail[0], nil
}

func (r *BinaryUint64Reader) ReadUint64() (uint64, error) {
	value, err := r.PeekUint64()
	if err != nil {
		return 0, err
	}

	r.valuesTail = r.valuesTail[1:]
	return value, nil
}

type SliceUint64Reader struct {
	data []uint64
	pos  int
}

func NewSliceUint64Reader(data []uint64) *SliceUint64Reader {
	return &SliceUint64Reader{data, 0}
}

func (w *SliceUint64Reader) SetProfiler(p *util.SimpleProfiler) {}

func (r *SliceUint64Reader) ReadUint64() (uint64, error) {
	if r.pos == len(r.data) {
		return 0, io.EOF
	}

	value := r.data[r.pos]
	r.pos++
	return value, nil
}

func (r *SliceUint64Reader) Data() []uint64 {
	return r.data[r.pos:]
}

func ReadUint64To(r Uint64Reader, out *uint64, err *error) bool {
	*out, *err = r.ReadUint64()
	switch *err {
	case nil:
		return true
	case io.EOF:
		*err = nil
		return false
	default:
		return false
	}
}

type BoundedUint64Reader struct {
	impl   Uint64Reader
	length uint64
	read   uint64
}

func NewBoundedUint64Reader(r Uint64Reader, length uint64) *BoundedUint64Reader {
	return &BoundedUint64Reader{
		impl:   r,
		length: length,
		read:   0,
	}
}

func (w *BoundedUint64Reader) SetProfiler(p *util.SimpleProfiler) {
	w.impl.SetProfiler(p)
}

func (r *BoundedUint64Reader) ReadUint64() (uint64, error) {
	if r.read == r.length {
		return 0, io.EOF
	}
	r.read++
	return r.impl.ReadUint64()
}

type TextUint64Reader struct {
	stream   *bufio.Reader
	profiler *util.SimpleProfiler
}

func NewTextUint64ReaderCount(r io.Reader, count int) *TextUint64Reader {
	stream, ok := r.(*bufio.Reader)
	if !ok {
		stream = bufio.NewReaderSize(r, count*SizeOfValue)
	}
	return &TextUint64Reader{
		stream:   stream,
		profiler: util.NewNilSimpleProfiler(),
	}
}

func (r *TextUint64Reader) SetProfiler(p *util.SimpleProfiler) {
	r.profiler = p
}

func (r *TextUint64Reader) ReadUint64() (uint64, error) {
	var value uint64
	r.profiler.StartMeasuring()
	_, err := fmt.Fscan(r.stream, &value)
	r.profiler.FinishMeasuring()
	if err != nil {
		return 0, err
	}
	return value, nil
}

//func (r BoundedUint64Reader) PeekUint64() (uint64, error) {
//	if r.read == r.length {
//		return 0, io.EOF
//	}
//	return r.impl.PeekUint64()
//}

//func HasValues(r Uint64Reader) bool {
//	_, err := r.PeekUint64()
//	return err == nil
//}
