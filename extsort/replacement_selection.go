package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"github.com/xosmig/extsort/util"
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
		}

		lastWrittenValue = currentHeap.HPop()
		if err := w.WriteUint64(lastWrittenValue); err != nil {
			return err
		}
		elementsWritten++

		return nil
	}

	var err error
	var readValue uint64
	for sortio.ReadUint64To(r, &readValue, &err) {
		if currentHeap.Cap() == 0 {
			err := flushOneElement()
			if err != nil {
				return nil, err
			}
		}

		if readValue >= lastWrittenValue {
			currentHeap.HPush(readValue)
		} else {
			nextHeap.HPush(readValue)
		}
	}
	if err != nil {
		return nil, err
	}

	for currentHeap.Len() > 0 || nextHeap.Len() > 0 {
		err = flushOneElement()
		if err != nil {
			return nil, err
		}
	}

	err = addSegment(Segment{segmentBegin, elementsWritten - segmentBegin})
	if err != nil {
		return nil, err
	}

	w.Flush()
	return segments, nil
}
