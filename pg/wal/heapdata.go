package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"fmt"

	"github.com/MediaMath/keryxlib/pg"
)

// HeapData describes heap resource manager specific details from a record
type HeapData interface {
	TablespaceID() uint32
	DatabaseID() uint32
	RelationID() uint32
	FromBlock() uint32
	FromOffset() uint16
	ToBlock() uint32
	ToOffset() uint16
	fmt.Stringer
}

// NewHeapData will interpret the heap data based on record type
func NewHeapData(recordType RecordType, isInit bool, data []byte) []HeapData {
	switch recordType {
	case Insert:
		return []HeapData{InsertData(data)}
	case Update:
		return []HeapData{UpdateData(data)}
	case Delete:
		return []HeapData{DeleteData(data)}
	case MultiInsert:
		return parseMultiInsertData(isInit, data)
	}

	return nil
}

// InsertData reads heap data as an insert
type InsertData []byte

// TablespaceID is the id of the tablespace this tuple is found in
func (d InsertData) TablespaceID() uint32 { return uint32(pg.LUint(d[0:4])) }

// DatabaseID is the id of the database this tuple is found in
func (d InsertData) DatabaseID() uint32 { return uint32(pg.LUint(d[4:8])) }

// RelationID is the id of the relation this tuple is found in
func (d InsertData) RelationID() uint32 { return uint32(pg.LUint(d[8:12])) }

// FromBlock is not available for inserts
func (d InsertData) FromBlock() uint32 { return 0 }

// FromOffset is not available for inserts
func (d InsertData) FromOffset() uint16 { return 0 }

// ToBlock is the page number where this tuple now resides
func (d InsertData) ToBlock() uint32 { return readBlockID(d[12:16]) }

// ToOffset is the item number where this tuple now resides
func (d InsertData) ToOffset() uint16 { return uint16(pg.LUint(d[16:18])) }

func (d InsertData) String() string {
	return fmt.Sprintf("Insert in %v/%v/%v to (%v,%v)", d.TablespaceID(), d.DatabaseID(), d.RelationID(), d.ToBlock(), d.ToOffset())
}

// UpdateData reads heap data as an update
type UpdateData []byte

// TablespaceID is the id of the tablespace this tuple is found in
func (d UpdateData) TablespaceID() uint32 { return uint32(pg.LUint(d[0:4])) }

// DatabaseID is the id of the database this tuple is found in
func (d UpdateData) DatabaseID() uint32 { return uint32(pg.LUint(d[4:8])) }

// RelationID is the id of the relation this tuple is found in
func (d UpdateData) RelationID() uint32 { return uint32(pg.LUint(d[8:12])) }

// FromBlock is the page number of the old version of this tuple
func (d UpdateData) FromBlock() uint32 { return readBlockID(d[12:16]) }

// FromOffset is the item number of the old version of this tuple
func (d UpdateData) FromOffset() uint16 { return uint16(pg.LUint(d[16:18])) }

// ToBlock is the page number of the new version of this tuple
func (d UpdateData) ToBlock() uint32 { return readBlockID(d[20:24]) }

// ToOffset is item number of the new version of this tuple
func (d UpdateData) ToOffset() uint16 { return uint16(pg.LUint(d[24:26])) }

func (d UpdateData) String() string {
	return fmt.Sprintf("Update in %v/%v/%v from (%v,%v) to (%v,%v)", d.TablespaceID(), d.DatabaseID(), d.RelationID(), d.FromBlock(), d.FromOffset(), d.ToBlock(), d.ToOffset())
}

// DeleteData reads heap data as a delete
type DeleteData []byte

// TablespaceID is the id of the tablespace this tuple is found in
func (d DeleteData) TablespaceID() uint32 { return uint32(pg.LUint(d[0:4])) }

// DatabaseID is the id of the database this tuple is found in
func (d DeleteData) DatabaseID() uint32 { return uint32(pg.LUint(d[4:8])) }

// RelationID is the id of the relation this tuple is found in
func (d DeleteData) RelationID() uint32 { return uint32(pg.LUint(d[8:12])) }

// FromBlock is the page number where this tuple previously resided
func (d DeleteData) FromBlock() uint32 { return readBlockID(d[12:16]) }

// FromOffset is the item number where this tuple previously resided
func (d DeleteData) FromOffset() uint16 { return uint16(pg.LUint(d[16:18])) }

// ToBlock is not available for deletes
func (d DeleteData) ToBlock() uint32 { return 0 }

// ToOffset is not available for deletes
func (d DeleteData) ToOffset() uint16 { return 0 }

func (d DeleteData) String() string {
	return fmt.Sprintf("Delete in %v/%v/%v from (%v,%v)", d.TablespaceID(), d.DatabaseID(), d.RelationID(), d.FromBlock(), d.FromOffset())
}

// MultiInsertData reads heap data as an insert
type MultiInsertData struct {
	tablespaceID uint32
	databaseID   uint32
	relationID   uint32
	toBlock      uint32
	toOffset     uint16
}

// TablespaceID is the id of the tablespace this tuple is found in
func (d MultiInsertData) TablespaceID() uint32 { return d.tablespaceID }

// DatabaseID is the id of the database this tuple is found in
func (d MultiInsertData) DatabaseID() uint32 { return d.databaseID }

// RelationID is the id of the relation this tuple is found in
func (d MultiInsertData) RelationID() uint32 { return d.relationID }

// FromBlock is not available for inserts
func (d MultiInsertData) FromBlock() uint32 { return 0 }

// FromOffset is not available for inserts
func (d MultiInsertData) FromOffset() uint16 { return 0 }

// ToBlock is the page number where this tuple now resides
func (d MultiInsertData) ToBlock() uint32 { return d.toBlock }

// ToOffset is the item number where this tuple now resides
func (d MultiInsertData) ToOffset() uint16 { return d.toOffset }

func (d MultiInsertData) String() string {
	return fmt.Sprintf("MultiInsert in %v/%v/%v to (%v,%v)", d.TablespaceID(), d.DatabaseID(), d.RelationID(), d.ToBlock(), d.ToOffset())
}

func parseMultiInsertData(isInit bool, d []byte) (multiInserts []HeapData) {
	const XlogHeapInitPage = 128

	var (
		tablespaceID = uint32(pg.LUint(d[0:4]))
		databaseID   = uint32(pg.LUint(d[4:8]))
		relationID   = uint32(pg.LUint(d[8:12]))
		toBlock      = uint32(pg.LUint(d[12:16]))
		flags        = d[16]
		ntuples      = uint16(pg.LUint(d[18:20]))
	)

	isInit = isInit || flags&XlogHeapInitPage > 0

	for i := uint16(0); i < ntuples; i++ {
		if isInit {
			multiInserts = append(multiInserts, MultiInsertData{tablespaceID, databaseID, relationID, toBlock, i + 1})
		} else {
			var (
				start    = i*2 + 20
				end      = start + 2
				toOffset = uint16(pg.LUint(d[start:end]))
			)

			multiInserts = append(multiInserts, MultiInsertData{tablespaceID, databaseID, relationID, toBlock, toOffset})
		}
	}

	return
}

func readBlockID(bs []byte) uint32 {
	return (uint32(pg.LUint(bs[0:2])) << 16) + uint32(pg.LUint(bs[2:4]))
}
