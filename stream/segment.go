package stream

import (
	"fmt"
	"time"
)

const (
	segmentMagic   = "BTLS"
	segmentVersion = 1
)

// Segment is the binary unit representing a set of page writes.
type Segment struct {
	Header SegmentHeader
	Pages  []PageFrame
	Data   []byte
}

// SegmentHeader stores metadata written alongside a segment.
type SegmentHeader struct {
	Magic             string            `json:"magic" cbor:"magic"`
	Version           int               `json:"version" cbor:"version"`
	TxID              uint64            `json:"txId" cbor:"txId"`
	ParentTxID        uint64            `json:"parentTxId" cbor:"parentTxId"`
	PageCount         int               `json:"pageCount" cbor:"pageCount"`
	PageSize          int               `json:"pageSize" cbor:"pageSize"`
	Checksum          uint64            `json:"checksum" cbor:"checksum"`
	Compression       CompressionType   `json:"compression" cbor:"compression"`
	CompressionLevel  int               `json:"compressionLevel,omitempty" cbor:"compressionLevel,omitempty"`
	CompressionWindow int               `json:"compressionWindow,omitempty" cbor:"compressionWindow,omitempty"`
	CreatedAt         time.Time         `json:"createdAt" cbor:"createdAt"`
	HighWaterMark     uint64            `json:"highWaterMark" cbor:"highWaterMark"`
	AdditionalAttrs   map[string]string `json:"additionalAttrs,omitempty" cbor:"additionalAttrs,omitempty"`
}

// Snapshot represents a complete copy of the database file.
type Snapshot struct {
	Header SnapshotHeader
	Data   []byte
}

// SnapshotHeader describes a snapshot artefact.
type SnapshotHeader struct {
	Magic             string          `json:"magic" cbor:"magic"`
	Version           int             `json:"version" cbor:"version"`
	TxID              uint64          `json:"txId" cbor:"txId"`
	PageCount         uint64          `json:"pageCount" cbor:"pageCount"`
	PageSize          int             `json:"pageSize" cbor:"pageSize"`
	Compression       CompressionType `json:"compression" cbor:"compression"`
	CompressionLevel  int             `json:"compressionLevel,omitempty" cbor:"compressionLevel,omitempty"`
	CompressionWindow int             `json:"compressionWindow,omitempty" cbor:"compressionWindow,omitempty"`
	CreatedAt         time.Time       `json:"createdAt" cbor:"createdAt"`
}

// PageFrame captures a single page and its payload.
type PageFrame struct {
	ID       uint64
	Overflow uint32
	Data     []byte
}

// EncodeHeader marshals the segment header to bytes.
func (h SegmentHeader) Encode() ([]byte, error) {
	if h.Magic == "" {
		h.Magic = segmentMagic
	}
	if h.Version == 0 {
		h.Version = segmentVersion
	}
	return cborEncMode.Marshal(h)
}

// DecodeSegmentHeader reads a JSON-encoded header and performs basic validation.
func DecodeSegmentHeader(buf []byte) (SegmentHeader, error) {
	var header SegmentHeader
	if err := cborDecMode.Unmarshal(buf, &header); err != nil {
		return SegmentHeader{}, fmt.Errorf("decode segment header: %w", err)
	}
	if header.Magic != segmentMagic {
		return SegmentHeader{}, fmt.Errorf("invalid segment magic: %s", header.Magic)
	}
	if header.Version != segmentVersion {
		return SegmentHeader{}, fmt.Errorf("unsupported segment version: %d", header.Version)
	}
	return header, nil
}

// Encode marshals a snapshot header to bytes.
func (h SnapshotHeader) Encode() ([]byte, error) {
	if h.Magic == "" {
		h.Magic = segmentMagic
	}
	if h.Version == 0 {
		h.Version = segmentVersion
	}
	return cborEncMode.Marshal(h)
}

// DecodeSnapshotHeader parses a JSON snapshot header.
func DecodeSnapshotHeader(buf []byte) (SnapshotHeader, error) {
	var header SnapshotHeader
	if err := cborDecMode.Unmarshal(buf, &header); err != nil {
		return SnapshotHeader{}, fmt.Errorf("decode snapshot header: %w", err)
	}
	if header.Magic != segmentMagic {
		return SnapshotHeader{}, fmt.Errorf("invalid snapshot magic: %s", header.Magic)
	}
	if header.Version != segmentVersion {
		return SnapshotHeader{}, fmt.Errorf("unsupported snapshot version: %d", header.Version)
	}
	return header, nil
}

func marshalSnapshot(snapshot *Snapshot) ([]byte, error) {
	payload := struct {
		Header SnapshotHeader `json:"header" cbor:"header"`
		Data   []byte         `json:"data" cbor:"data"`
	}{
		Header: snapshot.Header,
		Data:   snapshot.Data,
	}
	return cborEncMode.Marshal(payload)
}

func marshalSegment(segment *Segment) ([]byte, error) {
	payload := struct {
		Header SegmentHeader `json:"header" cbor:"header"`
		Data   []byte        `json:"data" cbor:"data"`
	}{
		Header: segment.Header,
		Data:   segment.Data,
	}
	return cborEncMode.Marshal(payload)
}
