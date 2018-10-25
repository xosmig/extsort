package main

import (
	"container/heap"
	sortio "github.com/xosmig/extsort/io"
	"io"
)

func DoMultiwayMerge(rs []sortio.Uint64Reader, w sortio.Uint64Writer) error {
	h := readersHeap{}
	for _, r := range rs {
		value, err := r.ReadUint64()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		heap.Push(&h, readerValuePair{r, value})
	}

	for h.Len() > 0 {
		pair := heap.Pop(&h).(readerValuePair)
		err := w.WriteUint64(pair.value)
		if err != nil {
			return err
		}
		pair.value, err = pair.reader.ReadUint64()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}

		heap.Push(&h, pair)
	}

	w.Flush()
	return nil
}

type readerValuePair struct {
	reader sortio.Uint64Reader
	value  uint64
}

type readersHeap struct{ data []readerValuePair }

func (r readersHeap) Len() int           { return len(r.data) }
func (r readersHeap) Less(i, j int) bool { return r.data[i].value < r.data[j].value }
func (r readersHeap) Swap(i, j int)      { r.data[i], r.data[j] = r.data[j], r.data[i] }

func (r *readersHeap) Push(x interface{}) {
	r.data = append(r.data, x.(readerValuePair))
}

func (h *readersHeap) Pop() interface{} {
	length := len(h.data)
	value := h.data[length-1]
	h.data = h.data[:length-1]
	return value
}
