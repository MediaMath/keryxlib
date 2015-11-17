package streams

import (
	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg/wal"
)

type TxnBuffer struct {
	f                      filters.MessageFilter
	bufferWorkingDirectory string
}

func (b *TxnBuffer) Start(entryChan <-chan *wal.Entry) (<-chan []*wal.Entry, error) {
	txns := make(chan []*wal.Entry)

	go func() {
		buffer := message.NewBuffer(b.bufferWorkingDirectory, 10*1024*wal.EntryBytesSize, wal.EntryBytesSize)
		var lastEntry *wal.Entry
		for entry := range entryChan {
			if lastEntry != nil && lastEntry.ReadFrom.Offset() > entry.ReadFrom.Offset() {
				continue
			} else if entry.Type == wal.Unknown {
				continue
			} else if b.f.FilterRelId(entry.RelationID) {
				continue
			}

			lastEntry = entry

			if entry.Type == wal.Commit {
				entriesBytes := buffer.Remove(entry.TransactionID)
				var entries []*wal.Entry
				for _, entryBytes := range entriesBytes {
					e := wal.EntryFromBytes(entryBytes)
					entries = append(entries, &e)
				}
				entries = append(entries, entry)
				txns <- entries
			} else if entry.Type == wal.Abort {
				buffer.Remove(entry.TransactionID)
			} else {
				buffer.Add(entry.TransactionID, entry.ToBytes())
			}

		}
		close(txns)
	}()

	return txns, nil
}
