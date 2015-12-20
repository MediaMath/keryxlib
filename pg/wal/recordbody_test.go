package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "testing"

const (
	pageSize               = 128
	fileSize               = 1024
	alignedHeaderSize      = 32
	numberLargerThanEight  = 50
	numberSmallerThanEight = 6
)

func TestShortBodyAfterHeader(t *testing.T) {
	headerLocation := getLocationAt(pageSize - (alignedHeaderSize + 8))
	block, body := createBlock(numberLargerThanEight, headerLocation)
	bodyLocation := getLocationAt(pageSize - 8)
	if len := body.AppendBodyAfterHeader(block, bodyLocation); len != 8 {
		t.Fatalf("expected 8 body bytes read but found %v", len)
	}
}

func TestCompleteBodyAfterHeader(t *testing.T) {
	headerLocation := getLocationAt(pageSize - (alignedHeaderSize + 8))
	block, body := createBlock(alignedHeaderSize+numberSmallerThanEight, headerLocation)
	bodyLocation := getLocationAt(pageSize - 8)
	if len := body.AppendBodyAfterHeader(block, bodyLocation); len != numberSmallerThanEight {
		t.Fatalf("expected %v body bytes read but found %v", numberSmallerThanEight, len)
	}
}

func TestNoContinuation(t *testing.T) {
	headerLocation := getLocationAt(pageSize - (alignedHeaderSize + 8))
	_, body := createBlock(alignedHeaderSize+numberLargerThanEight, headerLocation)
	page := createPageWithoutCont()
	if len := body.AppendContinuation(page); len != 0 {
		t.Fatalf("expected 0 cont bytes read but found %v", len)
	}
}

func TestContinuationNotComplete(t *testing.T) {
	headerLocation := getLocationAt(pageSize - (alignedHeaderSize + 8))
	_, body := createBlock(alignedHeaderSize+numberLargerThanEight, headerLocation)
	partialBodySize := uint64(numberLargerThanEight - 1)
	page := createPageWithCont(numberLargerThanEight - 1)
	if len := body.AppendContinuation(page); len != partialBodySize {
		t.Fatalf("expected %v cont bytes read but found %v", partialBodySize, len)
	}
}

func TestContinuationComplete(t *testing.T) {
	headerLocation := getLocationAt(pageSize - (alignedHeaderSize + 8))
	_, body := createBlock(alignedHeaderSize+numberLargerThanEight, headerLocation)
	page := createPageWithCont(numberLargerThanEight)
	if len := body.AppendContinuation(page); len != numberLargerThanEight {
		t.Fatalf("expected %v cont bytes read but found %v", numberLargerThanEight, len)
	}
}

func TestUnknownHeapData(t *testing.T) {
	headerLocation := getLocationAt(pageSize - (alignedHeaderSize + 8))
	block, body := createBlock(alignedHeaderSize+numberSmallerThanEight, headerLocation)
	bodyLocation := getLocationAt(pageSize - 8)
	body.AppendBodyAfterHeader(block, bodyLocation)
	if hd := body.HeapData(); hd != nil {
		t.Fatalf("expected unknown heap data but found %v", hd)
	}
}

func TestReadBodyEmptyBlock(t *testing.T) {
	testReadBody(t, 0, 0, 0, 0)
}

func TestReadBodyLocationBeyondBlock(t *testing.T) {
	testReadBody(t, 10, 20, 0, 0)
}

func TestReadBodyLengthFitsBlock(t *testing.T) {
	testReadBody(t, 10, 0, 10, 10)
}

func TestReadBodyLengthOverflowsBlock(t *testing.T) {
	testReadBody(t, 10, 0, 20, 10)
}

func testReadBody(t *testing.T, blockSize, bodyOffset, bodyLength, expected uint64) {
	block := make([]byte, blockSize)
	body := readBody(block, getLocationAt(bodyOffset), bodyLength)
	bodyLen := uint64(len(body))

	if bodyLen != expected {
		t.Fatalf("expected read size to be %v but found %v", expected, bodyLen)
	}
}

func getLocationAt(offsetInPage uint64) Location {
	return NewLocation(offsetInPage, 0, fileSize, pageSize, 8)
}

func createBlock(totalLength uint32, readFrom Location) ([]byte, *RecordBody) {
	block := make([]byte, pageSize)
	offset := readFrom.FromStartOfPage()

	block[offset+16] = byte(totalLength & 0x000000ff)
	block[offset+17] = byte((totalLength & 0x0000ffff) >> 8)
	block[offset+18] = byte((totalLength & 0x00ffffff) >> 16)
	block[offset+19] = byte(totalLength >> 24)

	header := NewRecordHeader(block, readFrom)

	body := NewRecordBody(header)

	return block, body
}

func createPageWithCont(contLength uint32) Page {
	block := make([]byte, pageSize)

	block[2] = 1

	block[16] = byte(contLength & 0x000000ff)
	block[17] = byte((contLength & 0x0000ffff) >> 8)
	block[18] = byte((contLength & 0x00ffffff) >> 16)
	block[19] = byte(contLength >> 24)

	return Page{block}
}

func createPageWithoutCont() Page {
	block := make([]byte, pageSize)

	return Page{block}
}
