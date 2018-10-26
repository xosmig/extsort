package extsort

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

func (h readersHeap) Len() int           { return len(h.data) }
func (h readersHeap) Less(i, j int) bool { return h.data[i].value < h.data[j].value }
func (h readersHeap) Swap(i, j int)      { h.data[i], h.data[j] = h.data[j], h.data[i] }

func (h *readersHeap) Push(x interface{}) {
	h.data = append(h.data, x.(readerValuePair))
}

func (h *readersHeap) Pop() interface{} {
	length := len(h.data)
	value := h.data[length-1]
	h.data = h.data[:length-1]
	return value
}
