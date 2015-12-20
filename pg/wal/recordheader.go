package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "github.com/MediaMath/keryxlib/pg"

// These constants describe the type of heap tuple found in the WAL
const (
	Unknown = iota // Unknown describes an entry in the WAL that is not interesting to us
	Insert         // Insert describes a tuple being inserted into a heap
	Update         // Update describes a tuple being updated in the heap
	Delete         // Delete describes a tuple being deleted from the heap
	Commit         // Commit describes a transaction being committed
	Abort          // Abort describes a transaction being aborted
)

// RecordType is a constant representing how an xlog record should be interpreted
type RecordType uint8

// RecordHeaderSize is the size of a record header before alignment
const RecordHeaderSize = 26

// RecordHeader contains methods to read fields of an xlog record header
type RecordHeader struct {
	readFrom Location
	bs       []byte
}

// NewRecordHeader creates a new RecordHeader from a block and a location
func NewRecordHeader(block []byte, location Location) *RecordHeader {
	start := location.FromStartOfPage()
	end := start + RecordHeaderSize

	if end > uint64(len(block)) {
		return nil
	}

	return &RecordHeader{location, block[start:end]}
}

// Crc is the crc of the record
func (r RecordHeader) Crc() uint32 {
	return uint32(pg.LUint(r.bs[0:4]))
}

// Previous is the location of the record that preceeds this one
func (r RecordHeader) Previous() Location {
	return LocationFromUint32s(uint32(pg.LUint(r.bs[4:8])), uint32(pg.LUint(r.bs[8:12])))
}

// TransactionID is the transaction that this record is apart of
func (r RecordHeader) TransactionID() uint32 {
	return uint32(pg.LUint(r.bs[12:16]))
}

// TotalLength is the length of the body after the header but before the next record
func (r RecordHeader) TotalLength() uint32 {
	return uint32(pg.LUint(r.bs[16:20]))
}

// Length is the length of resource manager specific data after the header
func (r RecordHeader) Length() uint32 {
	return uint32(pg.LUint(r.bs[20:24]))
}

// Info contains resource manager specific data
func (r RecordHeader) Info() uint8 {
	return uint8(pg.LUint(r.bs[24:25]))
}

// ResourceManagerID is the ID of the resource manager that created this record
func (r RecordHeader) ResourceManagerID() uint8 {
	return uint8(pg.LUint(r.bs[25:26]))
}

// Type indicates how the resource data should be interpreted
func (r RecordHeader) Type() RecordType {
	combined := uint16(r.ResourceManagerID())<<8 + uint16(r.Info()&0x70)

	switch combined {
	case 0x0100:
		return Commit
	case 0x0120:
		return Abort
	case 0x0A00:
		return Insert
	case 0x0A10:
		return Delete
	case 0x0A20:
		return Update
	case 0x0A40:
		return Update // HOT
	}

	return Unknown
}

// AlignedSize will return the size of the header plus alignment
func (r RecordHeader) AlignedSize() uint64 {
	return r.readFrom.Aligned().Add(RecordHeaderSize).Aligned().Difference(r.readFrom.Aligned())
}
