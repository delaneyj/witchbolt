package stream

import (
	"bytes"
	"testing"
)

func TestCompressionRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("stream-replication"), 32)
	cases := []CompressionConfig{
		{Codec: CompressionNone},
		{Codec: CompressionZSTD, Level: 6},
	}
	for _, cfg := range cases {
		cfg := cfg
		settings := cfg.normalized()
		t.Run(string(settings.Codec), func(t *testing.T) {
			compressed, err := compressBuffer(settings, payload)
			if err != nil {
				t.Fatalf("compress: %v", err)
			}
			out, err := decompressBuffer(settings.Codec, compressed)
			if err != nil {
				t.Fatalf("decompress: %v", err)
			}
			if !bytes.Equal(payload, out) {
				t.Fatalf("round trip mismatch for codec %s", settings.Codec)
			}
		})
	}
}

func TestCompressionLevelNormalization(t *testing.T) {
	if level := normalizeZSTDLevel(15); level != 22 {
		t.Fatalf("expected zstd level 22 got %d", level)
	}
	if level := normalizeZSTDLevel(2); level != -3 {
		t.Fatalf("expected zstd mapped to -3 got %d", level)
	}
}
