package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"testing"
)

var _ Uint64Reader = new(BinaryUint64Reader)

func writeRandomDataBinary(w *bufio.Writer, count uint64) error {
	buf := make([]byte, 8)

	for i := uint64(0); i < count; i++ {
		value := rand.Uint64()
		binary.LittleEndian.PutUint64(buf, value)
		_, err := w.Write(buf)
		if err != nil {
			return err
		}
	}

	return w.Flush()
}

func newTmpFileName() string {
	return fmt.Sprintf("tmp_%v", rand.Uint32())
}

func createRandomFile(count uint64) (*os.File, func(), error) {
	filename := newTmpFileName()

	f, err := os.Create(filename)
	if err != nil {
		return nil, func() {}, err
	}
	dispose := func() {
		f.Close()
		os.Remove(filename)
	}

	err = writeRandomDataBinary(bufio.NewWriterSize(f, 1024*1024), count)
	if err != nil {
		return nil, dispose, err
	}

	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, dispose, err
	}

	return f, dispose, nil
}

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
	_, disposeFile, err := createRandomFile(10)
	defer disposeFile()
	if err != nil {
		t.Fatalf("createRandomFile: %v", err)
	}
}
