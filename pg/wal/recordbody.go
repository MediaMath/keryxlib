package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// RecordBody collects the bytes that make up the body of a record
type RecordBody struct {
	header      *RecordHeader
	bs          []byte
	whatsNeeded uint64
	typ         RecordType
}

// NewRecordBody creates a new RecordBody based on a RecordHeader
func NewRecordBody(recordHeader *RecordHeader) *RecordBody {
	whatsNeeded := uint64(recordHeader.TotalLength()) - recordHeader.AlignedSize()

	return &RecordBody{header: recordHeader, whatsNeeded: whatsNeeded, typ: recordHeader.Type()}
}

// AppendBodyAfterHeader reads what is available of the body on the same page and appends it to the body
func (r *RecordBody) AppendBodyAfterHeader(block []byte, location Location) uint64 {
	body := readBody(block, location, r.whatsNeeded)
	r.bs = append(r.bs, body...)

	return uint64(len(body))
}

// AppendContinuation reads a continuation from a page and appends it to the body
func (r *RecordBody) AppendContinuation(page Page) uint64 {
	cont := page.Continuation()
	if cont == nil {
		return 0
	}

	r.bs = append(r.bs, cont...)

	return uint64(len(cont))
}

// IsComplete indicates that needs to be read of a body has been read
func (r *RecordBody) IsComplete() bool {
	if uint64(len(r.bs)) >= r.whatsNeeded {
		return true
	}

	return false
}

// HeapData interprets the body based on the type indicated in the record header
func (r *RecordBody) HeapData() []HeapData {
	return NewHeapData(r.typ, r.header.IsInit(), r.bs)
}

func readBody(block []byte, location Location, length uint64) []byte {
	var start, blockLen, remaining, end uint64

	start = location.FromStartOfPage()
	blockLen = uint64(len(block))
	remaining = blockLen - start

	if start >= blockLen {
		return block[0:0]
	}

	if length > remaining {
		end = start + remaining
	} else {
		end = start + length
	}

	return block[start:end]
}
