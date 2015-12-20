package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "testing"

const (
	defaultTimelineID = 0x00000001
	defaultFileSize   = 16 * 1024 * 1024
	defaultPageSize   = 8 * 1024
	defaultWordSize   = 8
)

func TestAlignmentExpectations(t *testing.T) {
	for _, exp := range alignmentExpectations {
		failIfAlignmentNotMatching(t, exp)
	}
}

func TestLocationExpectations(t *testing.T) {
	for _, exp := range locationExpectations {
		failIfLocationNotMatching(t, exp)
	}
}

var alignmentExpectations = []alignmentExpectation{
	{4, 0, 0},
	{4, 1, 4},
	{4, 2, 4},
	{4, 3, 4},
	{8, 0, 0},
	{8, 1, 8},
	{8, 2, 8},
	{8, 3, 8},
	{8, 4, 8},
	{8, 5, 8},
	{8, 6, 8},
	{8, 7, 8},
}

type alignmentExpectation struct {
	wordSize      uint32
	before, after uint64
}

func failIfAlignmentNotMatching(t *testing.T, exp alignmentExpectation) {
	actualLoc := loc(exp.before)
	actualLoc.wordSize = exp.wordSize
	actual := actualLoc.Aligned().offset
	if exp.after != actual {
		t.Errorf("expected aligned value of 0x%.8X to be 0x%.8X but got 0x%.8X", exp.before, exp.after, actual)
	}
}

var locationExpectations = []locationExpectation{
	{
		0x00000000038ceba0,
		"000000010000000000000003",
		0x00000000, 0x00000003, 0x038ceba0,
		0x008ceba0, 0x00731460, 0x00000ba0, 0x00001460,
		0x0000000003000000, 0x0000000004000000, 0x0000000002000000,
		0x00000000038ce000, 0x00000000038d0000, 0x00000000038cc000,
	}, {
		0xdefacebeefc0ffee,
		"00000001DEFACEBE000000EF",
		0xdefacebe, 0x000000ef, 0xefc0ffee,
		0x00c0ffee, 0x003f0012, 0x00001fee, 0x00000012,
		0xdefacebeef000000, 0xdefacebef0000000, 0xdefacebeee000000,
		0xdefacebeefc0e000, 0xdefacebeefc10000, 0xdefacebeefc0c000,
	}, {
		0x0000000014000578,
		"000000010000000000000014",
		0x00000000, 0x00000014, 0x14000578,
		0x00000578, 0x00FFFA88, 0x00000578, 0x00001A88,
		0x0000000014000000, 0x0000000015000000, 0x0000000013000000,
		0x0000000014000000, 0x0000000014002000, 0x0000000013FFE000,
	},
}

type locationExpectation struct {
	locationToTest                                             uint64
	filename                                                   string
	logID, segmentID, recordOffset                             uint32
	fromStartOfFile, toEndOfFile, fromStartOfPage, toEndOfPage uint64
	startOfFile, startOfNextFile, startOfPreviousFile          uint64
	startOfPage, startOfNextPage, startOfPreviousPage          uint64
}

func failIfLocationNotMatching(t *testing.T, exp locationExpectation) {
	ltt := loc(exp.locationToTest)

	if filename := ltt.Filename(); exp.filename != filename {
		t.Errorf("expected %q but got %q for filename", exp.filename, filename)
	}
	if logID := ltt.LogID(); exp.logID != logID {
		t.Errorf("expected 0x%.8x but got 0x%.8x for log id", exp.logID, logID)
	}
	if segmentID := ltt.SegmentID(); exp.segmentID != segmentID {
		t.Errorf("expected 0x%.8x but got 0x%.8x for segment id", exp.segmentID, segmentID)
	}
	if recordOffset := ltt.RecordOffset(); exp.recordOffset != recordOffset {
		t.Errorf("expected 0x%.8x but got 0x%.8x for record id", exp.recordOffset, recordOffset)
	}
	if fromStartOfFile := ltt.FromStartOfFile(); exp.fromStartOfFile != fromStartOfFile {
		t.Errorf("expected 0x%.8x but got 0x%.8x for from start of file", exp.fromStartOfFile, fromStartOfFile)
	}
	if toEndOfFile := ltt.ToEndOfFile(); exp.toEndOfFile != toEndOfFile {
		t.Errorf("expected 0x%.8x but got 0x%.8x for to end of file", exp.toEndOfFile, toEndOfFile)
	}
	if fromStartOfPage := ltt.FromStartOfPage(); exp.fromStartOfPage != fromStartOfPage {
		t.Errorf("expected 0x%.8x but got 0x%.8x for from start of page", exp.fromStartOfPage, fromStartOfPage)
	}
	if toEndOfPage := ltt.ToEndOfPage(); exp.toEndOfPage != toEndOfPage {
		t.Errorf("expected 0x%.8x but got 0x%.8x for to end of page", exp.toEndOfPage, toEndOfPage)
	}
	if startOfFile := ltt.StartOfFile(); loc(exp.startOfFile) != startOfFile {
		t.Errorf("expected %v but got %v for start of file", exp.startOfFile, startOfFile)
	}
	if startOfNextFile := ltt.StartOfNextFile(); loc(exp.startOfNextFile) != startOfNextFile {
		t.Errorf("expected %v but got %v for start of next file", exp.startOfNextFile, startOfNextFile)
	}
	if startOfPreviousFile := ltt.StartOfPreviousFile(); loc(exp.startOfPreviousFile) != startOfPreviousFile {
		t.Errorf("expected %v but got %v for start of previous file", exp.startOfPreviousFile, startOfPreviousFile)
	}
	if startOfPage := ltt.StartOfPage(); loc(exp.startOfPage) != startOfPage {
		t.Errorf("expected %v but got %v for start of page", exp.startOfPage, startOfPage)
	}
	if startOfNextPage := ltt.StartOfNextPage(); loc(exp.startOfNextPage) != startOfNextPage {
		t.Errorf("expected %v but got %v for start of next page", exp.startOfNextPage, startOfNextPage)
	}
	if startOfPreviousPage := ltt.StartOfPreviousPage(); loc(exp.startOfPreviousPage) != startOfPreviousPage {
		t.Errorf("expected %v but got %v for start of previous page", exp.startOfPreviousPage, startOfPreviousPage)
	}
}

func loc(l uint64) Location {
	return NewLocation(l, defaultTimelineID, defaultFileSize, defaultPageSize, defaultWordSize)
}
