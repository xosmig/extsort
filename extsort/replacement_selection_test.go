package extsort

import (
	sortio "github.com/xosmig/extsort/io"
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
