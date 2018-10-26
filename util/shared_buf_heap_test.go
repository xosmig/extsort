package util

import (
	"math/rand"
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
		t.Errorf("Expected Len: %v, actual: %v", len(items), h.Len())
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

func BenchmarkNewSharedBufHeap_10M(b *testing.B) {
	const N = 10 * 1000 * 1000
	randomData := make([]uint64, N)
	for i := range randomData {
		randomData[i] = rand.Uint64()
	}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		_, h := NewSharedBufHeap(N)
		b.StartTimer()

		for i := 0; i < N; i++ {
			h.HPush(randomData[i])
		}
		for i := 0; i < N; i++ {
			h.HPop()
		}
	}
}
