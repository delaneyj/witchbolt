package command

import "github.com/valyala/bytebufferpool"

func minSizedBuffer(size int) *bytebufferpool.ByteBuffer {
	buf := bytebufferpool.Get()
	if len(buf.B) < size {
		buf.Set(make([]byte, size))
	}
	return buf
}

func sizedBytes(size int) ([]byte, *bytebufferpool.ByteBuffer) {
	buf := minSizedBuffer(size)
	return buf.Bytes()[:size], buf
}
