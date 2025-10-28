package stream

import (
	cbor "github.com/fxamacker/cbor/v2"
)

type segmentPayload struct {
	Header SegmentHeader `json:"header" cbor:"header"`
	Pages  []PageFrame   `json:"pages" cbor:"pages"`
}

var (
	cborEncMode, _ = cbor.CanonicalEncOptions().EncMode()
	cborDecMode, _ = cbor.DecOptions{TimeTag: cbor.DecTagOptional}.DecMode()
)

func buildSegmentPayload(seg *Segment) segmentPayload {
	payload := segmentPayload{
		Header: seg.Header,
		Pages:  make([]PageFrame, len(seg.Pages)),
	}
	copy(payload.Pages, seg.Pages)
	payload.Header.PageCount = len(payload.Pages)
	return payload
}

func encodeSegmentCBORPayload(payload *segmentPayload) ([]byte, error) {
	return cborEncMode.Marshal(payload)
}
