package extsort

import (
	sortio "github.com/xosmig/extsort/io"
	"log"
	"math"
	"math/rand"
	"sort"
	"testing"
)

// 48s
func BenchmarkDoInitialSort_200M_values(b *testing.B) {
	const N = 200 * 1000 * 1000
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}

		input := sortio.NewSliceUint64Reader(inputData)
		output := sortio.NewNullUint64Writer()
		b.StartTimer()

		DoInitialSort(input, output, 100*1000*1000, math.MaxInt32)
	}
}

// 250s
// Warning: you can easily run out of memory while running this benchmark
func BenchmarkDoInitialSort_1G_values(b *testing.B) {
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

		DoInitialSort(input, output, MemoryLimit, math.MaxInt32)
	}
}

func benchmarkSortSliceImpl(b *testing.B, count int) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		log.Println("Preparing data...")
		inputData := make([]uint64, count)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}
		log.Println("Preparation finished.")
		b.StartTimer()

		sort.Slice(inputData, func(i, j int) bool { return inputData[i] < inputData[j] })
	}
}

// 32s
func BenchmarkSortSlice_1GiB(b *testing.B) {
	const N = 1024 * 1024 * 1024 / sortio.SizeOfValue // 1GiB data = 128M values
	benchmarkSortSliceImpl(b, N)
}

// 22s
func BenchmarkSortSlice_100M_values(b *testing.B) {
	const N = 100 * 1000 * 1000
	benchmarkSortSliceImpl(b, N)
}

// 45s
func BenchmarkSortSlice_200M_values(b *testing.B) {
	const N = 200 * 1000 * 1000
	benchmarkSortSliceImpl(b, N)
}

func BenchmarkSortSlice_200M_values_sorted(b *testing.B) {
	const N = 200 * 1000 * 1000
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		log.Println("Preparing data...")
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = uint64(i)
		}
		log.Println("Preparation finished.")
		b.StartTimer()

		sort.Slice(inputData, func(i, j int) bool { return inputData[i] < inputData[j] })
	}
}

// 35s
func BenchmarkSortFloat64_100M_values(b *testing.B) {
	const N = 100 * 1000 * 1000
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]float64, N)
		for i := range inputData {
			inputData[i] = float64(rand.Uint64())
		}
		b.StartTimer()

		sort.Float64s(inputData)
	}
}

func BenchmarkSortFloat64_200M_values(b *testing.B) {
	const N = 200 * 1000 * 1000
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]float64, N)
		for i := range inputData {
			inputData[i] = float64(rand.Uint64())
		}
		b.StartTimer()

		sort.Float64s(inputData)
	}
}

// 35s
// sort.Sort(Uint64Slice(inputData)) is more than 1.5 times slower than sort.Slice(...)
func BenchmarkUint64Slice_Sort_100M_values(b *testing.B) {
	const N = 100 * 1000 * 1000
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		log.Println("Initializing...")
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}
		log.Println("Finished initialization")
		b.StartTimer()

		//Uint64Slice(inputData).Sort()
		sort.Sort(Uint64Slice(inputData))
	}
}

// >70s
func BenchmarkUint64Slice_Sort_200M_values(b *testing.B) {
	const N = 200 * 1000 * 1000
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		b.StopTimer()
		inputData := make([]uint64, N)
		for i := range inputData {
			inputData[i] = rand.Uint64()
		}
		b.StartTimer()

		sort.Sort(Uint64Slice(inputData))
	}
}

// Uint64Slice attaches the methods of Interface to []uint64, sorting in increasing order.
type Uint64Slice []uint64

func (p Uint64Slice) Len() int           { return len(p) }
func (p Uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Sort is a convenience method.
func (p Uint64Slice) Sort() { sort.Sort(p) }
