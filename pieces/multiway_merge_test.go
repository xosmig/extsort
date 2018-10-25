package main

import (
	sortio "github.com/xosmig/extsort/io"
	"reflect"
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
