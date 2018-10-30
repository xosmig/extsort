package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"github.com/xosmig/extsort/util"
	"log"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestDoMultiwayMergeSort(t *testing.T) {
	testcases := []struct {
		inputData []uint64
		params    Params
		name      string
	}{
		{
			inputData: []uint64{2326, 141, 15, 824, 2, 1882, 344, 152, 85, 5, 123, 123, 1, 268, 1023, 9652},
			params: Params{
				MemoryLimit:                  1000,
				Arity:                        3,
				ReserveMemoryForSegmentsInfo: 100,
				FirstStageMemoryLimit:        3,
				BufferSize:                   1,
				UseReplacementSelection:      false,
			},
			name: "small_initialSort",
		},
		{
			inputData: []uint64{2326, 141, 15, 824, 2, 1882, 344, 152, 85, 5, 123, 123, 1, 268, 1023, 9652},
			params: Params{
				MemoryLimit:                  1000,
				Arity:                        3,
				ReserveMemoryForSegmentsInfo: 100,
				FirstStageMemoryLimit:        3,
				BufferSize:                   1,
				UseReplacementSelection:      true,
			},
			name: "small_replacementSelection",
		},
		{
			inputData: generateRandomArray(10 * 1024 * 1024),
			params:    DefaultParams(1024 * 1024),
			name:      "10M_randomValues_initialSort",
		},
		{
			inputData: generateRandomArray(10 * 1024 * 1024),
			params:    CreateParams(1024*1024, DefaultBufferSize, true),
			name:      "10M_randomValues_replacementSelection",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(tt *testing.T) {
			input := sortio.NewSliceUint64Reader(tc.inputData)
			output := sortio.NewSliceUint64Writer()

			err := DoMultiwayMergeSortParams(input, output, tc.params, util.NewNilSimpleProfiler())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expectedOutput := make([]uint64, len(tc.inputData))
			copy(expectedOutput, tc.inputData)
			sort.Slice(expectedOutput, func(i, j int) bool { return expectedOutput[i] < expectedOutput[j] })

			if len(expectedOutput) != len(output.Data()) {
				t.Fatalf("expected output length: %v, actual: %v", len(expectedOutput), len(output.Data()))
			}

			if !reflect.DeepEqual(expectedOutput, output.Data()) {
				if len(expectedOutput) < 20 {
					t.Errorf("expected output: %v, actual: %v", expectedOutput, output.Data())
				} else {
					t.Errorf("the actual output differs from the expected output")
				}
			}
		})
	}
}

// 500s
// Warning: you can easily run out of memory while running this benchmark
func BenchmarkDoMultiwayMergeSort_1G_values(b *testing.B) {
	const N = 8 * 1024 * 1024 * 1024 / sortio.SizeOfValue       // 8GiB data = 1G values
	const MemoryLimit = 1024 * 1024 * 1024 / sortio.SizeOfValue // 1 GiB data = 128M values

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		log.Println("Preparing data...")
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}

		input := sortio.NewSliceUint64Reader(inputData)
		output := sortio.NewNullUint64Writer()
		log.Println("Preparation finished.")
		b.StartTimer()

		DoMultiwayMergeSort(input, output, MemoryLimit, util.NewNilSimpleProfiler())
	}
}

func generateRandomArray(count int) []uint64 {
	result := make([]uint64, count)
	for i := range result {
		result[i] = rand.Uint64()
	}
	return result
}
