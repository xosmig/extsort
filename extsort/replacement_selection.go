package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"github.com/xosmig/extsort/util"
	"io"
)

type Segment struct {
	Begin, Length uint64
}

func DoReplacementSelection(
	r sortio.Uint64Reader,
	w sortio.Uint64Writer,
	heapMemoryLimit, segmentsMemoryLimit int) ([]Segment, error) {

	var currentHeap, nextHeap = util.NewSharedBufHeap(heapMemoryLimit)

	var lastWrittenValue uint64 = 0
	var elementsWritten uint64 = 0
	var segments []Segment
	var segmentBegin uint64 = 0

	addSegment := func(segment Segment) error {
		segments = append(segments, segment)
		if 2 * len(segments) > segmentsMemoryLimit {
			return ErrNotEnoughMemory
		}
		return nil
	}

	flushOneElement := func() error {
		if currentHeap.Len() == 0 {
			err := addSegment(Segment{segmentBegin, elementsWritten - segmentBegin})
			if err != nil {
				return err
			}

			lastWrittenValue = 0
			segmentBegin = elementsWritten
			currentHeap, nextHeap = nextHeap, currentHeap
			currentHeap.HInit()
		}

		lastWrittenValue = currentHeap.HPop()
		if err := w.WriteUint64(lastWrittenValue); err != nil {
			return err
		}
		elementsWritten++

		return nil
	}

	for currentHeap.Cap() > 0 {
		value, err := r.ReadUint64()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		currentHeap.ArrayPush(value)
	}
	currentHeap.HInit()

	var value uint64
	var err error
	for sortio.ReadUint64To(r, &value, &err) {
		//if value >= lastWrittenValue && value <= currentHeap.MinValue() {
		//	err := w.WriteUint64(value)
		//	if err != nil {
		//		return nil, err
		//	}
		//}

		if err = flushOneElement(); err != nil {
			return nil, err
		}

		if value >= lastWrittenValue {
			currentHeap.HPush(value)
		} else {
			nextHeap.ArrayPush(value)
		}
	}

	if err != nil {
		return nil, err
	}

	for currentHeap.Len() > 0 || nextHeap.Len() > 0 {
		if err = flushOneElement(); err != nil {
			return nil, err
		}
	}

	if err = addSegment(Segment{segmentBegin, elementsWritten - segmentBegin}); err != nil {
		return nil, err
	}

	err = w.Flush()
	if err != nil {
		return nil, err
	}

	return segments, nil
}
