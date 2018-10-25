package extsort

import (
	"container/heap"
	sortio "github.com/xosmig/extsort/io"
	"os"
)

type sortSegment struct {
	skipValues, count uint64
	filename          string
}

func (s *sortSegment) open() (*os.File, error) {
	f, err := os.Open(s.filename)
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(int64(s.skipValues*sortio.SizeOfValue), os.SEEK_SET)
	if err != nil {
		f.Close()
		return nil, err
	}

	return f, nil
}

func (s *sortSegment) getReader() (r sortio.Uint64Reader, dispose func(), err error) {
	var f *os.File
	f, err = s.open()
	if err != nil {
		return
	}

	r = sortio.NewBoundedUint64Reader(sortio.NewBinaryUint64Reader(f), s.count)
	dispose = func() { f.Close() }
	return
}

type sortSegmentsHeapImpl []sortSegment

func (h sortSegmentsHeapImpl) Len() int           { return len(h) }
func (h sortSegmentsHeapImpl) Less(i, j int) bool { return h[i].count < h[j].count }
func (h sortSegmentsHeapImpl) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *sortSegmentsHeapImpl) Push(x interface{}) {
	*h = append(*h, x.(sortSegment))
}

func (h *sortSegmentsHeapImpl) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type sortSegmentsHeap struct{ impl sortSegmentsHeapImpl }

func newSortSegmentsHeap(data []sortSegment) sortSegmentsHeap {
	h := sortSegmentsHeap{impl: sortSegmentsHeapImpl(data)}
	heap.Init(&h.impl)
	return h
}

func (h *sortSegmentsHeap) HPush(segment sortSegment) {
	heap.Push(&h.impl, segment)
}

func (h *sortSegmentsHeap) HPop() sortSegment {
	return heap.Pop(&h.impl).(sortSegment)
}

func (h *sortSegmentsHeap) Len() int {
	return h.impl.Len()
}
