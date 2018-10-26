package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"io"
)

func DoMultiwayMerge(readers []sortio.Uint64Reader, writer sortio.Uint64Writer) error {
	h := NewReadersHeap(len(readers))
	for _, r := range readers {
		value, err := r.ReadUint64()
		if err == io.EOF {
			continue
		}
		if err != nil {
			return err
		}
		h.Push(readerValuePair{r, value})
	}

	for h.Len() > 0 {
		pair := h.Top()
		err := writer.WriteUint64(pair.v)
		if err != nil {
			return err
		}
		value, err := pair.r.ReadUint64()
		if err == io.EOF {
			h.RemoveTop()
			continue
		}
		if err != nil {
			return err
		}

		pair.v = value
		h.FixTop()
	}

	writer.Flush()
	return nil
}

type readerValuePair struct {
	r sortio.Uint64Reader
	v uint64
}

type readersHeap struct {
	data []readerValuePair
}

func NewReadersHeap(cap int) *readersHeap {
	return &readersHeap{
		data: make([]readerValuePair, 0, cap),
	}
}

func (h *readersHeap) Push(x readerValuePair) {
	h.data = append(h.data, x)
	h.up(h.Len() - 1)
}

func (h *readersHeap) Top() *readerValuePair {
	return &h.data[0]
}

func (h *readersHeap) RemoveTop() {
	n := h.Len() - 1
	h.swap(0, n)
	h.down(0, n)
	h.popBack()
}

func (h *readersHeap) swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *readersHeap) Len() int {
	return len(h.data)
}

func (h *readersHeap) popBack() {
	h.data = h.data[:len(h.data)-1]
}

func (h *readersHeap) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || h.data[j].v >= h.data[i].v {
			break
		}
		h.swap(i, j)
		j = i
	}
}

func (h *readersHeap) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && h.data[j2].v < h.data[j1].v {
			j = j2 // = 2*i + 2  // right child
		}
		if h.data[j].v >= h.data[i].v {
			break
		}
		h.swap(i, j)
		i = j
	}
	return i > i0
}

func (h *readersHeap) FixTop() {
	h.down(0, h.Len())
}

//func DoMultiwayMerge(readers []sortio.Uint64Reader, writer sortio.Uint64Writer) error {
//	values := make([]uint64, len(readers))
//
//	h := util.NewIntHeap(len(readers), func(x, y int) bool { return values[x] < values[y] })
//	for idx, r := range readers {
//		var err error
//		values[idx], err = r.ReadUint64()
//		if err == io.EOF {
//			continue
//		}
//		if err != nil {
//			return err
//		}
//		h.Push(idx)
//	}
//
//	for h.Len() > 0 {
//		idx := h.Top()
//		err := writer.WriteUint64(values[idx])
//		if err != nil {
//			return err
//		}
//		value, err := readers[idx].ReadUint64()
//		if err == io.EOF {
//			h.Pop()
//			continue
//		}
//		if err != nil {
//			return err
//		}
//
//		values[idx] = value
//		h.FixTop()
//	}
//
//	writer.Flush()
//	return nil
//}
