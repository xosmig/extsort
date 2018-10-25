package extsort

import (
	"errors"
	"fmt"
	sortio "github.com/xosmig/extsort/io"
	"log"
	"math/rand"
	"os"
)

var ErrNotEnoughMemory = errors.New("not enough memory")

type Params struct {
	MemoryLimit                     int  // expressed in values (1 value equals 8 bytes)
	Arity                           int  // -1 for default value
	ReserveMemoryForSegmentsInfo    int  // expressed in values (1 value equals 8 bytes)
	ReplacementSelectionMemoryLimit int  // expressed in values (1 value equals 8 bytes)
	BufferSize                      int  // expressed in values (1 value equals 8 bytes)
}

const DefaultBufferSize = sortio.DefaultBufValuesCount

func DefaultParams(memoryLimit int) Params {
	return DefaultParamsBufferSize(memoryLimit, DefaultBufferSize)
}

func DefaultParamsBufferSize(memoryLimit, bufferSize int) Params {
	// this should be enough unless the input is more than a thousand times larger than the memory limit
	reserveMemoryForSegmentsInfo := 4096

	return Params{
		MemoryLimit:                     memoryLimit,
		Arity:                           -1,  // it will be calculated later by the sorting algorithm
		ReserveMemoryForSegmentsInfo:    reserveMemoryForSegmentsInfo,
		ReplacementSelectionMemoryLimit: memoryLimit - 2 * bufferSize - reserveMemoryForSegmentsInfo,
		BufferSize:                      bufferSize,
	}
}

var ErrExpectedPositiveValue = errors.New("expected positive value")

func validateParams(params Params) error {
	if params.MemoryLimit <= 0 || params.ReserveMemoryForSegmentsInfo <= 0 || params.ReplacementSelectionMemoryLimit <= 0 || params.BufferSize <= 0 {
		return ErrExpectedPositiveValue
	}

	if params.Arity <= 0 && params.Arity != -1 {
		return ErrExpectedPositiveValue
	}

	return nil
}

func DefaultArity(params Params, segmentsCount int) (int, error) {
	err := validateParams(params)
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

func DoMultiwayMergeSort(r sortio.Uint64Reader, w sortio.Uint64Writer, memorySize int) error {
	return DoMultiwayMergeSortParams(r, w, DefaultParams(memorySize))
}

func DoMultiwayMergeSortParams(r sortio.Uint64Reader, w sortio.Uint64Writer, params Params) error {
	s := sorter{
		params:  params,
		byteBuf: sortio.NewUint64ByteBuf(params.BufferSize),
	}
	defer s.close()
	return s.doSort(r, w)
}

type sorter struct {
	params   Params
	byteBuf  []byte
	tmpFiles []string
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
	return
}

func (s *sorter) close() {
	for _, filename := range s.tmpFiles {
		os.Remove(filename)
	}
}

func (s *sorter) doSort(r sortio.Uint64Reader, w sortio.Uint64Writer) error {
	err := validateParams(s.params)
	if err != nil {
		return err
	}

	log.Println("Running replacement selection...")
	segmentsHeap, err := s.runReplacementSelection(r)
	if err != nil {
		return err
	}
	log.Println("Replacement selection done.")

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

	log.Println("Replacement first merge...")
	firstMergeArity := (segmentsHeap.Len()-1)%(s.params.Arity-1) + 1
	if firstMergeArity > 1 {
		s.mergeSmallestSegments(&segmentsHeap, firstMergeArity)
	}
	log.Println("First merge done.")

	log.Println("Running intermediate merge sort...")
	for segmentsHeap.Len() > s.params.Arity {
		s.mergeSmallestSegments(&segmentsHeap, s.params.Arity)
	}
	log.Println("Intermediate merge sort done.")

	log.Println("Running final merge...")
	_, err = s.mergeSmallestSegmentsTo(&segmentsHeap, s.params.Arity, w)
	log.Println("Final merge done.")
	return err
}

func (s *sorter) runReplacementSelection(r sortio.Uint64Reader) (sortSegmentsHeap, error) {
	filename, w, f, err := s.newTmpFileWriter()
	if err != nil {
		return sortSegmentsHeap{}, err
	}
	defer f.Close()

	segments, err := DoReplacementSelection(r, w,
		s.params.ReplacementSelectionMemoryLimit, s.params.ReserveMemoryForSegmentsInfo)
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
		r, dispose, err := segment.getReader()
		if err != nil {
			return 0, err
		}
		defer dispose()

		readers = append(readers, r)
		outputLength += segment.count
	}

	err := DoMultiwayMerge(readers, w)
	if err != nil {
		return 0, err
	}

	return outputLength, nil
}
