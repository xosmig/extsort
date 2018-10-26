package io

const (
	DefaultBufValuesCount = 4096
	SizeOfValue           = 8
)

func NewUint64ByteBuf(count int) []byte {
	return make([]byte, count*SizeOfValue)
}

func CopyValues(r Uint64Reader, w Uint64Writer) error {
	var value uint64
	var err error
	for ReadUint64To(r, &value, &err) {
		err = w.WriteUint64(value)
		if err != nil {
			return err
		}
	}

	if err != nil {
		return err
	}

	return w.Flush()
}
