package wal

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
func NewHeapData(recordType RecordType, data []byte) HeapData {
	switch recordType {
	case Insert:
		return InsertData(data)
	case Update:
		return UpdateData(data)
	case Delete:
		return DeleteData(data)
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

func readBlockID(bs []byte) uint32 {
	return (uint32(pg.LUint(bs[0:2])) << 16) + uint32(pg.LUint(bs[2:4]))
}
