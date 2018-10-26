package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestDoMultiwayMerge(t *testing.T) {
	var input []sortio.Uint64Reader
	input = append(input, sortio.NewSliceUint64Reader([]uint64{1, 5, 123, 123, 268, 1023}))
	input = append(input, sortio.NewSliceUint64Reader([]uint64{}))
	input = append(input, sortio.NewSliceUint64Reader([]uint64{700, 1023, 1024}))
	input = append(input, sortio.NewSliceUint64Reader([]uint64{15, 141, 824, 1882, 2326, 9652}))
	input = append(input, sortio.NewSliceUint64Reader([]uint64{2, 85, 152, 344}))

	expectedOutput := []uint64{
		1, 2, 5, 15, 85, 123, 123, 141, 152, 268,
		344, 700, 824, 1023, 1023, 1024, 1882, 2326, 9652}
	output := sortio.NewSliceUint64Writer()

	DoMultiwayMerge(input, output)

	if !reflect.DeepEqual(expectedOutput, output.Data()) {
		t.Fatalf("expected output: %v, actual: %v", expectedOutput, output.Data())
	}
}

// (heap of pairs) 113s -> (heap of ints) 85s -> (copy-paste int heap from standard library) 33s ->
// (add IntHeap.FixTop) 24s -> (readersHeap + some optimizations in the heap interface) 15s
// Warning: you can easily run out of memory while running this benchmark
func BenchmarkDoMultiwayMerge_4GiB_values(b *testing.B) {
	const N = 4 * 1024 * 1024 * 1024 / sortio.SizeOfValue // 8GiB data = 1G values
	const InputCount = 8
	const ValuesPerInput = N / InputCount
	//defer profile.Start(profile.MemProfile).Stop()

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		log.Println("Preparing data...")

		input := make([]sortio.Uint64Reader, InputCount)
		for i := range input {
			inputPart := make([]uint64, ValuesPerInput)
			for j := range inputPart {
				inputPart[j] = rand.Uint64()
			}
			sort.Slice(inputPart, func(i, j int) bool { return inputPart[i] < inputPart[j] })
			input[i] = sortio.NewSliceUint64Reader(inputPart)
		}

		output := sortio.NewNullUint64Writer()

		log.Println("Preparation finished.")
		b.StartTimer()

		DoMultiwayMerge(input, output)
	}
}
