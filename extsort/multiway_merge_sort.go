package extsort

import (
	"errors"
	"fmt"
	sortio "github.com/xosmig/extsort/io"
	"github.com/xosmig/extsort/util"
	"log"
	"math/rand"
	"os"
	"runtime"
)

var ErrNotEnoughMemory = errors.New("not enough memory")

type ReplacementSelectionParams struct {
}

type Params struct {
	MemoryLimit                  int  // expressed in values (1 value equals 8 bytes)
	Arity                        int  // -1 for default value
	BufferSize                   int  // expressed in values (1 value equals 8 bytes)
	UseReplacementSelection      bool // InitialSort is used by default instead
	ReserveMemoryForSegmentsInfo int  // expressed in values (1 value equals 8 bytes). This parameter only used by replacement selection algorithm
	FirstStageMemoryLimit        int  // expressed in values (1 value equals 8 bytes)
}

const DefaultBufferSize = sortio.DefaultBufValuesCount

func DefaultParams(memoryLimit int) Params {
	return CreateParams(memoryLimit, DefaultBufferSize, false)
}

func CreateParams(memoryLimit int, bufferSize int, useReplacementSelection bool) Params {
	// this should be enough unless the input is more than a thousand times larger than the memory limit
	var reserveMemoryForSegmentsInfo = 4096

	return Params{
		MemoryLimit:                  memoryLimit,
		Arity:                        -1, // it will be calculated later by the sorting algorithm
		BufferSize:                   bufferSize,
		UseReplacementSelection:      useReplacementSelection,
		ReserveMemoryForSegmentsInfo: reserveMemoryForSegmentsInfo,
		FirstStageMemoryLimit:        memoryLimit - 2*bufferSize - reserveMemoryForSegmentsInfo,
	}
}

// TODO: more informative error
var ErrValueTooSmall = errors.New("parameter is too small")

func ValidateParams(params Params) error {
	if params.MemoryLimit < 1 || params.FirstStageMemoryLimit < 1 || params.BufferSize < 1 || params.ReserveMemoryForSegmentsInfo < 2 {
		return ErrValueTooSmall
	}

	if params.Arity < 2 && params.Arity != -1 {
		return ErrValueTooSmall
	}

	return nil
}

func DefaultArity(params Params, segmentsCount int) (int, error) {
	err := ValidateParams(params)
	if err != nil {
		return 0, err
	}

	memoryLeft := params.MemoryLimit
	// reserve memory for the output buffer
	memoryLeft -= params.BufferSize
	// reserve memory for the segments
	memoryLeft -= segmentsCount * 10
	// calculate arity
	arity := memoryLeft / params.BufferSize

	if arity <= 0 {
		return 0, ErrNotEnoughMemory
	}

	return arity, nil
}

func DoMultiwayMergeSort(
	r sortio.Uint64Reader,
	w sortio.Uint64Writer,
	memorySize int,
	profiler *util.SimpleProfiler) error {

	return DoMultiwayMergeSortParams(r, w, DefaultParams(memorySize), profiler)
}

func DoMultiwayMergeSortParams(
	r sortio.Uint64Reader,
	w sortio.Uint64Writer,
	params Params,
	profiler *util.SimpleProfiler) error {

	s := sorter{
		params:  params,
		byteBuf: sortio.NewUint64ByteBuf(params.BufferSize),
		profiler: profiler,
	}
	defer s.close()
	return s.doSort(r, w)
}

type sorter struct {
	params   Params
	byteBuf  []byte
	tmpFiles []string
	profiler *util.SimpleProfiler
}

func (s *sorter) newTmpFile() string {
	filename := fmt.Sprintf("sort_tmp_%v", rand.Uint32())
	s.tmpFiles = append(s.tmpFiles, filename)
	return filename
}

func (s *sorter) newTmpFileWriter() (filename string, w sortio.Uint64Writer, f *os.File, err error) {
	filename = s.newTmpFile()
	f, err = os.Create(filename)
	if err != nil {
		return
	}

	w = sortio.NewBinaryUint64WriterCountBuf(f, s.params.BufferSize, s.byteBuf)
	w.SetProfiler(s.profiler)
	return
}

func (s *sorter) close() {
	for _, filename := range s.tmpFiles {
		os.Remove(filename)
	}
}

func (s *sorter) doSort(r sortio.Uint64Reader, w sortio.Uint64Writer) error {
	err := ValidateParams(s.params)
	if err != nil {
		return err
	}

	log.Println("Running first stage...")
	segmentsHeap, err := s.runFirstStage(r)
	if err != nil {
		return err
	}
	log.Println("First stage done.")
	runtime.GC()

	if s.params.Arity == -1 {
		s.params.Arity, err = DefaultArity(s.params, segmentsHeap.Len())
		if err != nil {
			return err
		}
	}

	if segmentsHeap.Len() <= s.params.Arity {
		log.Println("Running final merge...")
		_, err := s.mergeSmallestSegmentsTo(&segmentsHeap, segmentsHeap.Len(), w)
		log.Println("Final merge done.")
		return err
	}

	log.Println("Running first merge...")
	firstMergeArity := (segmentsHeap.Len()-1)%(s.params.Arity-1) + 1
	if firstMergeArity > 1 {
		s.mergeSmallestSegments(&segmentsHeap, firstMergeArity)
	}
	log.Println("First merge done.")
	runtime.GC()

	log.Println("Running intermediate merge sort...")
	for segmentsHeap.Len() > s.params.Arity {
		s.mergeSmallestSegments(&segmentsHeap, s.params.Arity)
		runtime.GC()
	}
	log.Println("Intermediate merge sort done.")

	log.Println("Running final merge...")
	_, err = s.mergeSmallestSegmentsTo(&segmentsHeap, s.params.Arity, w)
	log.Println("Final merge done.")
	runtime.GC()

	return err
}

func (s *sorter) runFirstStage(r sortio.Uint64Reader) (sortSegmentsHeap, error) {
	filename, w, f, err := s.newTmpFileWriter()
	if err != nil {
		return sortSegmentsHeap{}, err
	}
	defer f.Close()

	var segments []Segment
	if s.params.UseReplacementSelection {
		segments, err = DoReplacementSelection(r, w,
			s.params.FirstStageMemoryLimit, s.params.ReserveMemoryForSegmentsInfo)
	} else {
		segments, err = DoInitialSort(r, w, s.params.FirstStageMemoryLimit, s.params.ReserveMemoryForSegmentsInfo)
	}
	if err != nil {
		return sortSegmentsHeap{}, err
	}

	var sortSegments []sortSegment
	for _, segment := range segments {
		sortSegments = append(sortSegments, sortSegment{segment.Begin, segment.Length, filename})
	}

	return newSortSegmentsHeap(sortSegments), nil
}

func (s *sorter) mergeSmallestSegments(h *sortSegmentsHeap, n int) error {
	filename, w, f, err := s.newTmpFileWriter()
	if err != nil {
		return err
	}
	defer f.Close()

	outputLength, err := s.mergeSmallestSegmentsTo(h, n, w)
	if err != nil {
		return err
	}

	h.HPush(sortSegment{0, outputLength, filename})
	return nil
}

func (s *sorter) mergeSmallestSegmentsTo(h *sortSegmentsHeap, n int, w sortio.Uint64Writer) (uint64, error) {
	var readers []sortio.Uint64Reader
	var outputLength uint64 = 0
	for i := 0; i < n; i++ {
		segment := h.HPop()
		r, f, err := s.getSegmentReader(&segment)
		if err != nil {
			return 0, err
		}
		defer f.Close()

		readers = append(readers, r)
		outputLength += segment.count
	}

	err := DoMultiwayMerge(readers, w)
	if err != nil {
		return 0, err
	}

	return outputLength, nil
}

func (s *sorter) getSegmentReader(segment *sortSegment) (sortio.Uint64Reader, *os.File, error) {
	f, err := segment.Open()
	if err != nil {
		return nil, nil, err
	}

	binaryReader := sortio.NewBinaryUint64ReaderCountBuf(f, s.params.BufferSize, s.byteBuf)
	reader := sortio.NewBoundedUint64Reader(binaryReader, segment.count)
	reader.SetProfiler(s.profiler)
	return reader, f, nil
}
