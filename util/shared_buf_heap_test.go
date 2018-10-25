package util

import (
	"reflect"
	"sort"
	"testing"
)

func testHeapSort(t *testing.T, h SharedBufHeap, items []uint64) {
	sorted := make([]uint64, len(items))
	copy(sorted, items)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, item := range items {
		h.HPush(item)
	}

	if h.Len() != len(items) {
		t.Errorf("Expected len: %v, actual: %v", len(items), h.Len())
	}

	var heapSorted []uint64
	for h.Len() > 0 {
		heapSorted = append(heapSorted, h.HPop())
	}

	if !reflect.DeepEqual(heapSorted, sorted) {
		t.Errorf("Invalid heap sort result. Expected: %v, actual: %v", sorted, heapSorted)
	}
}

func TestLeftHeap_HeapSort(t *testing.T) {
	items := []uint64{1, 7, 2, 5, 5, 12, 2, 4}
	leftHeap, _ := NewSharedBufHeap(len(items))
	testHeapSort(t, leftHeap, items)
}

func TestRightHeap_HeapSort(t *testing.T) {
	items := []uint64{1, 7, 2, 5, 5, 12, 2, 4}
	_, rightHeap := NewSharedBufHeap(len(items))
	testHeapSort(t, rightHeap, items)
}
