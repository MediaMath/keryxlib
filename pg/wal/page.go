package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "github.com/MediaMath/keryxlib/pg"

// Page contains methods for reading values from a WAL page header and for detecting/reading a continuation
type Page struct {
	bs []byte
}

// MagicValueIsValid indicates if a Page is correct or not
func (p Page) MagicValueIsValid() bool {
	return p.Is91() || p.Is94()
}

// Magic returns the format version of the page
func (p Page) Magic() uint16 {
	return uint16(p.bs[1])<<8 + uint16(p.bs[0])
}

// Is91 indicates if a Page is from version 9.1
func (p Page) Is91() bool {
	return p.Magic() == 0xD066
}

// Is94 indicates if a Page is from version 9.4
func (p Page) Is94() bool {
	return p.Magic() == 0xD07E
}

// Info can be used to determine if a page header is long (bit 2 is set) or if it contains a continuation (bit 1 is set)
func (p Page) Info() uint16 {
	return uint16(pg.LUint(p.bs[2:4]))
}

// TimelineID is the timeline this page is found on
func (p Page) TimelineID() uint32 {
	return uint32(pg.LUint(p.bs[4:8]))
}

// Location is the Location this page starts at
func (p Page) Location() Location {
	if p.Is91() {
		return LocationFromUint32s(uint32(pg.LUint(p.bs[8:12])), uint32(pg.LUint(p.bs[12:16])))
	}

	return LocationFromUint32s(uint32(pg.LUint(p.bs[12:16])), uint32(pg.LUint(p.bs[8:12])))
}

// SystemID can be used to determine if a page was written by a particular server
func (p Page) SystemID() uint64 {
	if p.IsLong() {
		return uint64(pg.LUint(p.bs[16:24]))
	}

	return 0
}

// SegmentSize is the size in bytes of a single WAL file
func (p Page) SegmentSize() uint32 {
	if p.IsLong() {
		return uint32(pg.LUint(p.bs[24:28]))
	}

	return 0
}

// BlockSize is the size of a page in a WAL file
func (p Page) BlockSize() uint32 {
	if p.IsLong() {
		return uint32(pg.LUint(p.bs[28:32]))
	}

	return 0
}

// Continuation will return the bytes of a continuation of the previous record's body if present on the page
func (p Page) Continuation() []byte {
	if p.IsCont() {
		var contStart, contEnd uint64

		if p.Is94() {
			contStart = p.HeaderLength()
			contEnd = contStart + pg.LUint(p.bs[16:20])
		} else {
			sizeOffset := p.HeaderLength()
			contStart = sizeOffset + 4
			contEnd = contStart + pg.LUint(p.bs[sizeOffset:contStart])
		}

		maxContEnd := uint64(len(p.bs))
		if contEnd > maxContEnd {
			contEnd = maxContEnd
		}

		return p.bs[contStart:contEnd]
	}

	return nil
}

// IsCont checks the page's info to see if it has a continuation record
func (p Page) IsCont() bool {
	return p.Info()&1 > 0
}

// IsLong checks if the page has extra fields which is typical in the beginning of a file
func (p Page) IsLong() bool {
	return p.Info()&2 > 0
}

// HeaderLength returns the size in bytes of the portion of the page used for its header
func (p Page) HeaderLength() uint64 {
	if p.Is94() {
		if p.IsLong() {
			return 40
		}

		return 24
	}

	if p.IsLong() {
		return 32
	}

	return 16
}
