package io

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/xosmig/extsort/util"
	"io"
	"strconv"
)

type Syncer interface {
	Sync() error
}

type WriteSyncer interface {
	io.Writer
	Syncer
}

type Uint64Writer interface {
	WriteUint64(x uint64) error
	Flush() error
	SetProfiler(p *util.SimpleProfiler)
}

type BinaryUint64Writer struct {
	stream    WriteSyncer
	valuesBuf []uint64
	writeBuf  []byte
	profiler  *util.SimpleProfiler
}

var ErrTooSmallBuffer = errors.New("too small buffer provided")

func NewBinaryUint64WriterCountBuf(w WriteSyncer, count int, byteBuffer []byte) *BinaryUint64Writer {
	if len(byteBuffer) < count*SizeOfValue {
		panic(ErrTooSmallBuffer)
	}

	return &BinaryUint64Writer{
		stream:    w,
		valuesBuf: make([]uint64, 0, count),
		writeBuf:  byteBuffer,
		profiler:  util.NewNilSimpleProfiler(),
	}
}

func NewBinaryUint64WriterCount(w WriteSyncer, count int) *BinaryUint64Writer {
	return NewBinaryUint64WriterCountBuf(w, count, NewUint64ByteBuf(count))
}

func NewBinaryUint64Writer(w WriteSyncer) *BinaryUint64Writer {
	return NewBinaryUint64WriterCount(w, DefaultBufValuesCount)
}

func (w *BinaryUint64Writer) SetProfiler(p *util.SimpleProfiler) {
	w.profiler = p
}

func (w *BinaryUint64Writer) Flush() error {
	count := len(w.valuesBuf)
	for valueIdx := 0; valueIdx < count; valueIdx++ {
		binary.LittleEndian.PutUint64(w.writeBuf[valueIdx*SizeOfValue:], w.valuesBuf[valueIdx])
	}

	var errWrite error = nil
	var errSync error = nil

	w.profiler.StartMeasuring()
	_, errWrite = w.stream.Write(w.writeBuf[:count*SizeOfValue])
	if errWrite == nil {
		errSync = w.stream.Sync()
	}
	w.profiler.FinishMeasuring()

	if errWrite != nil {
		return errWrite
	}

	if errSync != nil {
		return errSync
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

func (w *SliceUint64Writer) SetProfiler(p *util.SimpleProfiler) {}

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

func (w NullUint64Writer) SetProfiler(p *util.SimpleProfiler) {}

type TextUint64Writer struct {
	stream   *bufio.Writer
	profiler *util.SimpleProfiler
}

func NewTextUint64WriterCount(w io.Writer, count int) *TextUint64Writer {
	stream, ok := w.(*bufio.Writer)
	if !ok {
		stream = bufio.NewWriterSize(w, count*SizeOfValue)
	}

	return &TextUint64Writer{
		stream:   stream,
		profiler: util.NewNilSimpleProfiler(),
	}
}

func (w *TextUint64Writer) SetProfiler(p *util.SimpleProfiler) {
	w.profiler = p
}

func (w *TextUint64Writer) WriteUint64(x uint64) error {
	byteBuf := make([]byte, 0, 30)
	bufWithValue := strconv.AppendUint(byteBuf, x /*base=*/, 10)

	w.profiler.StartMeasuring()
	_, err := w.stream.Write(bufWithValue)
	w.profiler.FinishMeasuring()
	return err
}

func (w *TextUint64Writer) Flush() error {
	return w.stream.Flush()
}
