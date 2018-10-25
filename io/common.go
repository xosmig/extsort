package io

const (
	DefaultBufValuesCount = 4096
	SizeOfValue           = 8
)

func NewUint64ByteBuf(count int) []byte {
	return make([]byte, count*SizeOfValue)
}
