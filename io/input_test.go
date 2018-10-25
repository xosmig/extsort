package io

import (
	"io"
	"log"
	"testing"
)

var _ Uint64Reader = new(BinaryUint64Reader)

func readBenchmarkImpl(b *testing.B, count uint64) {
	b.StopTimer()
	log.Println("Preparing file")

	f, disposeFile, err := createRandomFile(count)
	defer disposeFile()
	if err != nil {
		b.Fatalf("createRandomFile: %v", err)
	}
	r := NewBinaryUint64Reader(f)

	log.Println("Reading file")
	b.StartTimer()

	err = nil
	valuesRead := uint64(0)
	for {
		_, err = r.ReadUint64()

		if err != nil {
			break
		} else {
			valuesRead++
		}
	}

	if err != io.EOF {
		b.Errorf("error reading data: %v", err)
	}

	if valuesRead != count {
		b.Errorf("expected: %v values, read: %v values", count, valuesRead)
	}
}

func BenchmarkBinaryUint64Reader_Read8G(b *testing.B) {
	const count = 1024 * 1024 * 1024

	log.Println("Benchmark started")
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		readBenchmarkImpl(b, count)
	}
	b.StopTimer()
	log.Println("Benchmark finished")
}

func TestBinaryUint64Reader_EOF(t *testing.T) {
	const count = 10

	f, disposeFile, err := createRandomFile(count)
	defer disposeFile()
	if err != nil {
		t.Fatalf("createRandomFile: %v", err)
	}

	r := NewBinaryUint64Reader(f)
	for i := 0; i < count; i++ {
		_, err := r.ReadUint64()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		value, err := r.ReadUint64()
		if err != io.EOF {
			if err == nil {
				t.Fatalf("expected EOF, got value: %v", value)
			} else {
				t.Fatalf("expected EOF, got error: %v", err)
			}
		}
	}
}
