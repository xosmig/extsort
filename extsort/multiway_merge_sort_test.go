package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestDoMultiwayMergeSort(t *testing.T) {
	testcases := []struct{
		inputData []uint64
		params    Params
		name      string
	}{
		{
			inputData: []uint64{2326, 141, 15, 824, 2, 1882, 344, 152, 85, 5, 123, 123, 1, 268, 1023, 9652},
			params: Params{
				MemoryLimit:                     1000,
				Arity:                           3,
				ReserveMemoryForSegmentsInfo:    100,
				ReplacementSelectionMemoryLimit: 2,
				BufferSize:                      1,
			},
			name: "small",
		},
		{
			inputData: generateRandomArray(10 * 1024 * 1024),
			params: DefaultParams(1024 * 1024),
			name: "10M_random_values",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(tt *testing.T) {
			input := sortio.NewSliceUint64Reader(tc.inputData)
			output := sortio.NewSliceUint64Writer()

			err := DoMultiwayMergeSortParams(input, output, tc.params)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expectedOutput := make([]uint64, len(tc.inputData))
			copy(expectedOutput, tc.inputData)
			sort.Slice(expectedOutput, func(i, j int) bool { return expectedOutput[i] < expectedOutput[j] })

			if !reflect.DeepEqual(expectedOutput, output.Data()) {
				t.Errorf("expected output: %v, actual: %v", expectedOutput, output.Data())
			}
		})
	}
}

func generateRandomArray(count int) []uint64 {
	result := make([]uint64, count)
	for i := range result {
		result[i] = rand.Uint64()
	}
	return result
}
