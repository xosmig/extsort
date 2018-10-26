package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestDoReplacementSelection(t *testing.T) {
	input := []uint64{2326, 141, 15, 824, 9652, 2, 1882, 344, 152, 85}
	expectedOutput := []uint64{
		15, 141, 824, 1882, 2326, 9652,
		2, 85, 152, 344}
	expectedSegments := []Segment{{0, 6}, {6, 4}}
	const memSize = 4

	r := sortio.NewSliceUint64Reader(input)
	w := sortio.NewSliceUint64Writer()

	segments, err := DoReplacementSelection(r, w, memSize, 100)
	if err != nil {
		t.Fatalf("error in replacement selection: %v", err)
	}

	if !reflect.DeepEqual(w.Data(), expectedOutput) {
		t.Errorf("expected: %v, actual: %v", expectedOutput, w.Data())
	}

	if !reflect.DeepEqual(segments, expectedSegments) {
		t.Errorf("expected barriers: %v, actual: %v", expectedSegments, segments)
	}
}

func BenchmarkDoReplacementSelection_10M_values(b *testing.B) {
	const N = 10 * 1000 * 1000

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}

		input := sortio.NewSliceUint64Reader(inputData)
		output := sortio.NewSliceUint64Writer()
		b.StartTimer()

		DoReplacementSelection(input, output, 1000*1000, math.MaxInt32)
	}
}

func BenchmarkDoReplacementSelection_200M_values(b *testing.B) {
	const N = 200 * 1000 * 1000

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}

		input := sortio.NewSliceUint64Reader(inputData)
		output := sortio.NewSliceUint64Writer()
		b.StartTimer()

		DoReplacementSelection(input, output, 100*1000*1000, math.MaxInt32)
	}
}

func BenchmarkDoReplacementSelection_200M_values_sorted(b *testing.B) {
	const N = 200 * 1000 * 1000

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = uint64(i)
		}

		input := sortio.NewSliceUint64Reader(inputData)
		output := sortio.NewSliceUint64Writer()
		b.StartTimer()

		DoReplacementSelection(input, output, 100*1000*1000, math.MaxInt32)
	}
}
