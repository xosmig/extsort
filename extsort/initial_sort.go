package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"io"
	"sort"
)

func DoInitialSort(
	r sortio.Uint64Reader,
	w sortio.Uint64Writer,
	bufferMemoryLimit, segmentsMemoryLimit int) ([]Segment, error) {

	var valuesBuf = make([]uint64, bufferMemoryLimit)

	var segments []Segment
	var segmentBegin uint64 = 0
	var err error
	for err != io.EOF {
		var count uint64
		count, err = doReadAndSort(r, w, valuesBuf)
		if err != nil && err != io.EOF {
			return nil, err
		}

		segments = append(segments, Segment{segmentBegin, count})
		segmentBegin += count

		if 2 * len(segments) > segmentsMemoryLimit {
			return nil, ErrNotEnoughMemory
		}
	}

	return segments, nil
}

func doReadAndSort(
	r sortio.Uint64Reader,
	w sortio.Uint64Writer,
	valuesBuf []uint64) (uint64, error) {

	//log.Println("Reading values...")
	var valuesRead = 0
	for valuesRead < len(valuesBuf) {
		value, err := r.ReadUint64()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}

		valuesBuf[valuesRead] = value
		valuesRead++
	}
	//log.Println("Read ", len(valuesBuf), " values.")

	if valuesRead == 0 {
		return 0, io.EOF
	}

	//log.Println("Sorting...")
	sort.Slice(valuesBuf[:valuesRead], func(i, j int) bool { return valuesBuf[i] < valuesBuf[j] })

	//log.Println("Writing...")
	for _, value := range valuesBuf[:valuesRead] {
		err := w.WriteUint64(value)
		if err != nil {
			return 0, err
		}
	}
	err := w.Flush()
	if err != nil {
		return 0, err
	}

	//log.Println("Done.")
	return uint64(valuesRead), nil
}
