package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "fmt"

// Location models a 64 bit address in the WAL
type Location struct {
	offset     uint64
	timelineID uint32
	fileSize   uint32
	pageSize   uint32
	wordSize   uint32
}

// NewLocation constructs a new location from an offset
func NewLocation(loc uint64, timelineID, fileSize, pageSize, wordSize uint32) Location {
	return Location{loc, timelineID, fileSize, pageSize, wordSize}
}

// NewLocationWithDefaults constructs a new location from an offset with common defaults
func NewLocationWithDefaults(loc uint64) Location {
	return Location{loc, 1, 16 * 1024 * 1024, 8 * 1024, 8}
}

// LocationFromUint32s constructs a location from two parts
func LocationFromUint32s(high, low uint32) Location {
	return NewLocationWithDefaults(uint64(high)<<32 + uint64(low))
}

func (l Location) String() string {
	return fmt.Sprintf("0x%.16x", l.offset)
}

// Offset returns the offset this location is based on
func (l Location) Offset() uint64 {
	return l.offset
}

// Filename is the name of the WAL segment file this location is in
func (l Location) Filename() string {
	return fmt.Sprintf("%.8X%.8X%.8X", l.timelineID, l.LogID(), l.SegmentID())
}

// LogID is the upper 32 bits of the location
func (l Location) LogID() uint32 {
	return uint32(l.offset >> 32)
}

// SegmentID is the segment id of the location
func (l Location) SegmentID() uint32 {
	return l.RecordOffset() / l.fileSize
}

// RecordOffset is the lower 32 bits of the location
func (l Location) RecordOffset() uint32 {
	return uint32(l.offset)
}

// Add increases the offset of the Location by some amount
func (l Location) Add(amount uint64) Location {
	out := NewLocation(l.offset+amount, l.timelineID, l.fileSize, l.pageSize, l.wordSize)
	maxSegments := 0xffffffff / l.fileSize
	if out.SegmentID() == maxSegments {
		out = out.Add(uint64(l.fileSize))
	}
	return out
}

// Subtract decreases the offset of the Location by some amount
func (l Location) Subtract(amount uint64) Location {
	out := NewLocation(l.offset-amount, l.timelineID, l.fileSize, l.pageSize, l.wordSize)
	maxSegments := 0xffffffff / l.fileSize
	if out.SegmentID() == maxSegments {
		out = out.Subtract(uint64(l.fileSize))
	}
	return out
}

// Difference calculates how much larger this offset is than another
func (l Location) Difference(other Location) uint64 {
	return l.offset - other.offset
}

// FromStartOfFile calculates the number of bytes from the start of the file to the location
func (l Location) FromStartOfFile() uint64 {
	return l.offset % uint64(l.fileSize)
}

// FromStartOfPage calculates the number of bytes from the start of the page to the location
func (l Location) FromStartOfPage() uint64 {
	return l.FromStartOfFile() % uint64(l.pageSize)
}

// ToEndOfFile calculates the number of bytes from the location to the end of the file
func (l Location) ToEndOfFile() uint64 {
	return uint64(l.fileSize) - l.FromStartOfFile()
}

// ToEndOfPage calculates the number of bytes from the location to the end of the page
func (l Location) ToEndOfPage() uint64 {
	return uint64(l.pageSize) - l.FromStartOfPage()
}

// StartOfFile calculates the location of the first byte in the file this location is in
func (l Location) StartOfFile() Location {
	return l.Subtract(l.FromStartOfFile())
}

// StartOfNextFile calculates the location of the first byte in the next file after the one this location is in
func (l Location) StartOfNextFile() Location {
	return l.StartOfFile().Add(uint64(l.fileSize))
}

// StartOfPreviousFile calculates the location of the first byte in the prevous file before the one this location is in
func (l Location) StartOfPreviousFile() Location {
	return l.StartOfFile().Subtract(uint64(l.fileSize))
}

// StartOfPage calculates the location of the first byte in the page this location is in
func (l Location) StartOfPage() Location {
	return l.Subtract(l.FromStartOfPage())
}

// StartOfNextPage calculates the location of the first byte in the next page after the one this location is in
func (l Location) StartOfNextPage() Location {
	return l.StartOfPage().Add(uint64(l.pageSize))
}

// StartOfPreviousPage calculates the location of the first byte in the previous page before the one this location is in
func (l Location) StartOfPreviousPage() Location {
	return l.StartOfPage().Subtract(uint64(l.pageSize))
}

// IsOnSamePageAs determines if a location is on the same page as this one
func (l Location) IsOnSamePageAs(other Location) bool {
	return l.StartOfPage() == other.StartOfPage()
}

// Aligned calculates the location aligned to the given word size
func (l Location) Aligned() Location {
	r := l.offset % uint64(l.wordSize)

	if r != 0 {
		return l.Add(uint64(l.wordSize) - r)
	}

	return l
}
