package wal

import (
	"fmt"
	"time"
	"unsafe"
)

// Entry contains the data extracted from insert/update/delete/commit records
type Entry struct {
	Type          RecordType
	ReadFrom      Location
	Previous      Location
	TimelineID    uint32
	LogID         uint32
	TransactionID uint32
	TablespaceID  uint32
	DatabaseID    uint32
	RelationID    uint32
	FromBlock     uint32
	FromOffset    uint16
	ToBlock       uint32
	ToOffset      uint16
	ParseTime     int64
}

//EntryBytesSize is the size of the entries.
const EntryBytesSize = 61

// ToBytes converts an entry to a slice of bytes
func (e Entry) ToBytes() []byte {
	timePtr := (*uint64)(unsafe.Pointer(&e.ParseTime))
	return []byte{
		byte(e.Type),
		byte(e.ReadFrom.offset >> 56),
		byte(e.ReadFrom.offset >> 48),
		byte(e.ReadFrom.offset >> 40),
		byte(e.ReadFrom.offset >> 32),
		byte(e.ReadFrom.offset >> 24),
		byte(e.ReadFrom.offset >> 16),
		byte(e.ReadFrom.offset >> 8),
		byte(e.ReadFrom.offset),
		byte(e.Previous.offset >> 56),
		byte(e.Previous.offset >> 48),
		byte(e.Previous.offset >> 40),
		byte(e.Previous.offset >> 32),
		byte(e.Previous.offset >> 24),
		byte(e.Previous.offset >> 16),
		byte(e.Previous.offset >> 8),
		byte(e.Previous.offset),
		byte(e.TimelineID >> 24),
		byte(e.TimelineID >> 16),
		byte(e.TimelineID >> 8),
		byte(e.TimelineID),
		byte(e.LogID >> 24),
		byte(e.LogID >> 16),
		byte(e.LogID >> 8),
		byte(e.LogID),
		byte(e.TransactionID >> 24),
		byte(e.TransactionID >> 16),
		byte(e.TransactionID >> 8),
		byte(e.TransactionID),
		byte(e.TablespaceID >> 24),
		byte(e.TablespaceID >> 16),
		byte(e.TablespaceID >> 8),
		byte(e.TablespaceID),
		byte(e.DatabaseID >> 24),
		byte(e.DatabaseID >> 16),
		byte(e.DatabaseID >> 8),
		byte(e.DatabaseID),
		byte(e.RelationID >> 24),
		byte(e.RelationID >> 16),
		byte(e.RelationID >> 8),
		byte(e.RelationID),
		byte(e.FromBlock >> 24),
		byte(e.FromBlock >> 16),
		byte(e.FromBlock >> 8),
		byte(e.FromBlock),
		byte(e.FromOffset >> 8),
		byte(e.FromOffset),
		byte(e.ToBlock >> 24),
		byte(e.ToBlock >> 16),
		byte(e.ToBlock >> 8),
		byte(e.ToBlock),
		byte(e.ToOffset >> 8),
		byte(e.ToOffset),
		byte(*timePtr >> 56),
		byte(*timePtr >> 48),
		byte(*timePtr >> 40),
		byte(*timePtr >> 32),
		byte(*timePtr >> 24),
		byte(*timePtr >> 16),
		byte(*timePtr >> 8),
		byte(*timePtr),
	}
}

// EntryFromBytes reconstructs an entry from a slice of bytes
func EntryFromBytes(bs []byte) Entry {
	parseTime := uint64(bs[53])<<56 + uint64(bs[54])<<48 + uint64(bs[55])<<40 + uint64(bs[56])<<32 + uint64(bs[57])<<24 + uint64(bs[58])<<16 + uint64(bs[59])<<8 + uint64(bs[60])

	return Entry{
		Type:          RecordType(bs[0]),
		ReadFrom:      NewLocationWithDefaults(uint64(bs[1])<<56 + uint64(bs[2])<<48 + uint64(bs[3])<<40 + uint64(bs[4])<<32 + uint64(bs[5])<<24 + uint64(bs[6])<<16 + uint64(bs[7])<<8 + uint64(bs[8])),
		Previous:      NewLocationWithDefaults(uint64(bs[9])<<56 + uint64(bs[10])<<48 + uint64(bs[11])<<40 + uint64(bs[12])<<32 + uint64(bs[13])<<24 + uint64(bs[14])<<16 + uint64(bs[15])<<8 + uint64(bs[16])),
		TimelineID:    uint32(bs[17])<<24 + uint32(bs[18])<<16 + uint32(bs[19])<<8 + uint32(bs[20]),
		LogID:         uint32(bs[21])<<24 + uint32(bs[22])<<16 + uint32(bs[23])<<8 + uint32(bs[24]),
		TransactionID: uint32(bs[25])<<24 + uint32(bs[26])<<16 + uint32(bs[27])<<8 + uint32(bs[28]),
		TablespaceID:  uint32(bs[29])<<24 + uint32(bs[30])<<16 + uint32(bs[31])<<8 + uint32(bs[32]),
		DatabaseID:    uint32(bs[33])<<24 + uint32(bs[34])<<16 + uint32(bs[35])<<8 + uint32(bs[36]),
		RelationID:    uint32(bs[37])<<24 + uint32(bs[38])<<16 + uint32(bs[39])<<8 + uint32(bs[40]),
		FromBlock:     uint32(bs[41])<<24 + uint32(bs[42])<<16 + uint32(bs[43])<<8 + uint32(bs[44]),
		FromOffset:    uint16(bs[45])<<8 + uint16(bs[46]),
		ToBlock:       uint32(bs[47])<<24 + uint32(bs[48])<<16 + uint32(bs[49])<<8 + uint32(bs[50]),
		ToOffset:      uint16(bs[51])<<8 + uint16(bs[52]),
		ParseTime:     int64(parseTime),
	}
}

// NewEntry builds an entry from a page, record header, record body and a location
func NewEntry(page *Page, recordHeader *RecordHeader, recordBody *RecordBody) *Entry {
	entry := &Entry{
		Type:          recordHeader.Type(),
		ReadFrom:      recordHeader.readFrom,
		Previous:      recordHeader.Previous(),
		TimelineID:    page.TimelineID(),
		LogID:         page.Location().LogID(),
		TransactionID: recordHeader.TransactionID(),
	}

	heapData := recordBody.HeapData()
	if heapData != nil {
		entry.TablespaceID = heapData.TablespaceID()
		entry.DatabaseID = heapData.DatabaseID()
		entry.RelationID = heapData.RelationID()
		entry.FromBlock = heapData.FromBlock()
		entry.FromOffset = heapData.FromOffset()
		entry.ToBlock = heapData.ToBlock()
		entry.ToOffset = heapData.ToOffset()
	}

	entry.ParseTime = time.Now().UnixNano()
	return entry
}

func (e Entry) String() string {
	switch e.Type {
	case Insert:
		return fmt.Sprintf("Insert into %v/%v/%v::(%v,%v) on transaction id %v read from %v/%v",
			e.TablespaceID, e.DatabaseID, e.RelationID, e.ToBlock, e.ToOffset, e.TransactionID, e.TimelineID, e.ReadFrom)
	case Update:
		return fmt.Sprintf("Update in %v/%v/%v::(%v,%v)->(%v,%v) on transaction id %v read from %v/%v",
			e.TablespaceID, e.DatabaseID, e.RelationID, e.FromBlock, e.FromOffset, e.ToBlock, e.ToOffset, e.TransactionID, e.TimelineID, e.ReadFrom)
	case Delete:
		return fmt.Sprintf("Delete from %v/%v/%v::(%v,%v) on transaction id %v read from %v/%v",
			e.TablespaceID, e.DatabaseID, e.RelationID, e.FromBlock, e.FromOffset, e.TransactionID, e.TimelineID, e.ReadFrom)
	case Commit:
		return fmt.Sprintf("Commit of transaction id %v read from %v/%v", e.TransactionID, e.TimelineID, e.ReadFrom)
	case Abort:
		return fmt.Sprintf("Abort of transaction id %v read from %v/%v", e.TransactionID, e.TimelineID, e.ReadFrom)
	}

	return fmt.Sprintf("Unknown WAL Entry read from %v/%v", e.TimelineID, e.ReadFrom)
}
