package stream

import (
	"math/rand"
	"testing"
)

func makePayload(size int) []byte {
	buf := make([]byte, size)
	rand.New(rand.NewSource(42)).Read(buf)
	return buf
}

func BenchmarkCompressNone(b *testing.B) {
	payload := makePayload(1 << 20) // 1 MiB
	settings := CompressionConfig{Codec: CompressionNone}.normalized()
	runCompressBench(b, payload, settings)
}

func BenchmarkCompressZSTD(b *testing.B) {
	payload := makePayload(1 << 20)
	settings := CompressionConfig{Codec: CompressionZSTD, Level: 6}.normalized()
	runCompressBench(b, payload, settings)
}

func runCompressBench(b *testing.B, payload []byte, settings compressionSettings) {
	b.ReportAllocs()
	compressed, err := compressBuffer(settings, payload)
	if err != nil {
		b.Fatalf("warmup compress: %v", err)
	}
	if _, err := decompressBuffer(settings.Codec, compressed); err != nil {
		b.Fatalf("warmup decompress: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := compressBuffer(settings, payload)
		if err != nil {
			b.Fatalf("compress failed: %v", err)
		}
		if _, err := decompressBuffer(settings.Codec, out); err != nil {
			b.Fatalf("decompress failed: %v", err)
		}
	}
}
