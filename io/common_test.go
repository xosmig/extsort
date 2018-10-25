package io

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
)

func TestBinaryUint64IO_WriteAndRead(t *testing.T) {
	f, disposeFile, err := createTmpFile()
	defer disposeFile()
	if err != nil {
		t.Fatalf("createRandomFile: %v", err)
	}

	data := []uint64{2326, 141, 15, 824, 9652, 2, 1882, 344, 152, 85}

	w := NewBinaryUint64Writer(f)
	for _, value := range data {
		err = w.WriteUint64(value)
		if err != nil {
			t.Fatalf("error writing data: %v", err)
		}
	}
	w.Flush()

	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		t.Fatalf("Seek: %v", err)
	}

	r := NewBinaryUint64Reader(f)
	var dataRead []uint64
	var value uint64
	for ReadUint64To(r, &value, &err) {
		dataRead = append(dataRead, value)
	}

	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}

	if !reflect.DeepEqual(data, dataRead) {
		t.Fatalf("expected: %v, got: %v", data, dataRead)
	}
}

func TestSliceUint64IO_WriteAndRead(t *testing.T) {
	data := []uint64{2326, 141, 15, 824, 9652, 2, 1882, 344, 152, 85}

	w := NewSliceUint64Writer()
	for _, value := range data {
		err := w.WriteUint64(value)
		if err != nil {
			t.Fatalf("error writing data: %v", err)
		}
	}

	r := NewSliceUint64Reader(w.Data())
	var dataRead []uint64

	var value uint64
	var err error
	for ReadUint64To(r, &value, &err) {
		dataRead = append(dataRead, value)
	}

	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}

	if !reflect.DeepEqual(data, dataRead) {
		t.Fatalf("expected: %v, got: %v", data, dataRead)
	}
}

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
	f, dispose, err := createTmpFile()
	if err != nil {
		return nil, dispose, err
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

func createTmpFile() (*os.File, func(), error) {
	filename := newTmpFileName()

	f, err := os.Create(filename)
	if err != nil {
		return nil, func() {}, err
	}
	dispose := func() {
		f.Close()
		os.Remove(filename)
	}

	return f, dispose, nil
}
