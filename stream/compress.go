package stream

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

type compressionSettings struct {
	Codec  CompressionType
	Level  int
	Window int
}

func normalizeCompressionSettings(settings compressionSettings) compressionSettings {
	switch settings.Codec {
	case CompressionZSTD:
		settings.Level = normalizeZSTDLevel(settings.Level)
		settings.Window = 0
	case CompressionNone:
		settings.Level = 0
		settings.Window = 0
	default:
		settings.Level = 0
		settings.Window = 0
	}
	return settings
}

func normalizeZSTDLevel(level int) int {
	if level <= 0 {
		return 0
	}
	if level > 11 {
		level = 11
	}
	table := []int{0, -5, -3, -1, 0, 3, 6, 9, 12, 15, 18, 22}
	return table[level]
}

func compressBuffer(settings compressionSettings, payload []byte) ([]byte, error) {
	switch settings.Codec {
	case CompressionNone:
		return append([]byte(nil), payload...), nil
	case CompressionZSTD:
		options := []zstd.EOption{}
		if settings.Level != 0 {
			options = append(options, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(settings.Level)))
		}
		encoder, err := zstd.NewWriter(nil, options...)
		if err != nil {
			return nil, fmt.Errorf("create zstd writer: %w", err)
		}
		defer encoder.Close()
		return encoder.EncodeAll(payload, make([]byte, 0, len(payload))), nil
	default:
		return nil, fmt.Errorf("unknown compression codec: %s", settings.Codec)
	}
}

func decompressBuffer(codec CompressionType, payload []byte) ([]byte, error) {
	switch codec {
	case CompressionNone:
		return payload, nil
	case CompressionZSTD:
		decoder, err := zstd.NewReader(bytes.NewReader(payload))
		if err != nil {
			return nil, fmt.Errorf("create zstd reader: %w", err)
		}
		defer decoder.Close()
		out, err := io.ReadAll(decoder)
		if err != nil {
			return nil, fmt.Errorf("zstd read: %w", err)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown compression codec: %s", codec)
	}
}
