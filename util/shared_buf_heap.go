package util

import "container/heap"

type SharedBufHeap interface {
	ArrayPush(uint64)
	HInit()
	HPush(uint64)
	HPop() uint64
	Len() int
	Cap() int
	MinValue() uint64
}

func NewSharedBufHeap(size int) (SharedBufHeap, SharedBufHeap) {
	heapData := &heapData{
		data:      make([]uint64, size, size),
		leftSize:  0,
		rightSize: 0,
	}
	return leftHeap{heapData}, rightHeap{heapData}
}

type heapData struct {
	data      []uint64
	leftSize  int
	rightSize int
}

func (h *heapData) Cap() int {
	return len(h.data) - h.leftSize - h.rightSize
}

func sliceSwap(v []uint64, i, j int) {
	v[i], v[j] = v[j], v[i]
}

type leftHeap struct{ *heapData }

func (h leftHeap) Len() int {
	return h.leftSize
}

func (h leftHeap) Less(i, j int) bool {
	return h.data[i] < h.data[j]
}

func (h leftHeap) Swap(i, j int) {
	sliceSwap(h.data, i, j)
}

func (h leftHeap) Push(x interface{}) {
	h.data[h.leftSize] = x.(uint64)
	h.leftSize++
}

func (h leftHeap) Pop() interface{} {
	h.leftSize--
	return h.data[h.leftSize]
}

func (h leftHeap) HPush(x uint64) {
	heap.Push(h, x)
}

func (h leftHeap) HPop() uint64 {
	return heap.Pop(h).(uint64)
}

func (h leftHeap) ArrayPush(x uint64) {
	h.Push(x)
}

func (h leftHeap) HInit() {
	heap.Init(h)
}

func (h leftHeap) MinValue() uint64 {
	return h.data[0]
}

type rightHeap struct{ *heapData }

func (h rightHeap) dataIdx(i int) int {
	return len(h.data) - 1 - i
}

func (h rightHeap) Len() int {
	return h.rightSize
}

func (h rightHeap) Less(i, j int) bool {
	return h.data[h.dataIdx(i)] < h.data[h.dataIdx(j)]
}

func (h rightHeap) Swap(i, j int) {
	sliceSwap(h.data, h.dataIdx(i), h.dataIdx(j))
}

func (h rightHeap) Push(x interface{}) {
	h.data[h.dataIdx(h.rightSize)] = x.(uint64)
	h.rightSize++
}

func (h rightHeap) Pop() interface{} {
	h.rightSize--
	return h.data[h.dataIdx(h.rightSize)]
}

func (h rightHeap) HPush(x uint64) {
	heap.Push(h, x)
}

func (h rightHeap) HPop() uint64 {
	return heap.Pop(h).(uint64)
}

func (h rightHeap) ArrayPush(x uint64) {
	h.Push(x)
}

func (h rightHeap) HInit() {
	heap.Init(h)
}

func (h rightHeap) MinValue() uint64 {
	return h.data[h.dataIdx(0)]
}
