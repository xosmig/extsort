package main

import "container/heap"

type heapData struct {
	data      []uint64
	leftSize  int
	rightSize int
}

func NewTwoSidedHeap(size int) (heap.Interface, heap.Interface) {
	heapData := &heapData{
		data:      make([]uint64, size, size),
		leftSize:  0,
		rightSize: 0,
	}
	return LeftHeap{heapData}, RightHeap{heapData}
}

func sliceSwap(v []uint64, i, j int) {
	v[i], v[j] = v[j], v[i]
}

type LeftHeap struct{ *heapData }

func (h LeftHeap) Len() int {
	return h.leftSize
}

func (h LeftHeap) Less(i, j int) bool {
	return h.data[i] < h.data[j]
}

func (h LeftHeap) Swap(i, j int) {
	sliceSwap(h.data, i, j)
}

func (h LeftHeap) Push(x interface{}) {
	h.data[h.leftSize] = x.(uint64)
	h.leftSize++
}

func (h LeftHeap) Pop() interface{} {
	h.leftSize--
	return h.data[h.leftSize]
}

type RightHeap struct{ *heapData }

func (h RightHeap) dataIdx(i int) int {
	return len(h.data) - 1 - i
}

func (h RightHeap) Len() int {
	return h.rightSize
}

func (h RightHeap) Less(i, j int) bool {
	return h.data[h.dataIdx(i)] < h.data[h.dataIdx(j)]
}

func (h RightHeap) Swap(i, j int) {
	sliceSwap(h.data, h.dataIdx(i), h.dataIdx(j))
}

func (h RightHeap) Push(x interface{}) {
	h.data[h.dataIdx(h.rightSize)] = x.(uint64)
	h.rightSize++
}

func (h RightHeap) Pop() interface{} {
	h.rightSize--
	return h.data[h.dataIdx(h.rightSize)]
}
