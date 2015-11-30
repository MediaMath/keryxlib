package streams

import (
	"fmt"
	"time"

	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg"
	"github.com/MediaMath/keryxlib/pg/wal"
)

//PopulatedMessageStream takes collections of commited WAL entries, organized by transaction and populates them from the db with their current values.  It then publishes them as a Transaction message.
type PopulatedMessageStream struct {
	f  filters.MessageFilter
	sr *pg.SchemaReader
}

func interestingEntryType(entry *wal.Entry) bool {
	return entry.Type == wal.Commit || entry.Type == wal.Insert || entry.Type == wal.Update || entry.Type == wal.Delete
}

func (b *PopulatedMessageStream) filterRelation(entry *wal.Entry) bool {
	return entry.RelationID > 0 && b.f.FilterRelID(entry.RelationID)
}

//Start begins async selecting on the WAL transaction buffer channel
func (b *PopulatedMessageStream) Start(serverVersion string, entryChan <-chan []*wal.Entry) (<-chan *message.Transaction, error) {
	txns := make(chan *message.Transaction)
	go func() {
		for entries := range entryChan {
			var messages []message.Message
			for _, entry := range entries {
				if interestingEntryType(entry) && !b.filterRelation(entry) {
					msg := createMessage(entry)
					b.populate(msg)
					messages = append(messages, *msg)
				}
			}

			txn := &message.Transaction{}
			txn.Messages = messages
			txn.ServerVersion = serverVersion

			commit := messages[len(messages)-1]
			txn.TransactionID = commit.TransactionID
			txn.CommitKey = commit.Key
			txn.FirstKey = messages[0].Key

			txns <- txn
		}
		close(txns)
	}()

	return txns, nil
}

func (b *PopulatedMessageStream) waitForLogToCatchUp(rvMsg *message.Message) {

	curLoc := uint64(rvMsg.LogID)<<32 + uint64(rvMsg.RecordOffset)

	lrl := b.sr.LatestReplayLocation()
	for curLoc > lrl {
		<-time.After(time.Second)
		lrl = b.sr.LatestReplayLocation()
	}
}

func (b *PopulatedMessageStream) populate(rvMsg *message.Message) {
	b.waitForLogToCatchUp(rvMsg)

	if rvMsg.Type == message.InsertMessage || rvMsg.Type == message.UpdateMessage || rvMsg.Type == message.DeleteMessage {
		rvMsg.DatabaseName = b.sr.GetDatabaseName(rvMsg.DatabaseID)
		rvMsg.Namespace, rvMsg.Relation = b.sr.GetNamespaceAndTable(rvMsg.DatabaseID, rvMsg.RelationID)
	}

	if rvMsg.Type == message.InsertMessage || rvMsg.Type == message.UpdateMessage {

		vs, err := b.sr.GetFieldValues(rvMsg.DatabaseID, rvMsg.RelationID, rvMsg.Block, rvMsg.Offset)
		if err != nil {
			rvMsg.PopulationError = err.Error()
		} else if vs == nil {
			rvMsg.PopulationError = fmt.Sprintf("Message skipped for no fields.")
		} else {
			for f, v := range vs {
				if !b.f.FilterColumn(rvMsg.RelFullName(), f.Column) {
					rvMsg.AppendField(f.Column, f.String(), v)
				}
			}
		}
	}

}

func createMessage(entry *wal.Entry) *message.Message {
	msg := new(message.Message)

	msg.TimelineID = entry.TimelineID

	msg.LogID = entry.ReadFrom.LogID()
	msg.RecordOffset = entry.ReadFrom.RecordOffset()
	msg.Key = message.NewKey(entry.TimelineID, entry.ReadFrom.LogID(), entry.ReadFrom.RecordOffset())
	msg.Prev = message.NewKey(entry.TimelineID, entry.Previous.LogID(), entry.Previous.RecordOffset())

	msg.TransactionID = entry.TransactionID
	msg.TablespaceID = entry.TablespaceID
	msg.DatabaseID = entry.DatabaseID
	msg.RelationID = entry.RelationID

	msg.Fields = make([]message.Field, 0)

	switch entry.Type {
	case wal.Insert:
		msg.Type = message.InsertMessage
		msg.Block = entry.ToBlock
		msg.Offset = entry.ToOffset

	case wal.Update:
		msg.Type = message.UpdateMessage
		msg.Block = entry.ToBlock
		msg.Offset = entry.ToOffset

	case wal.Delete:
		msg.Type = message.DeleteMessage
		msg.Block = entry.FromBlock
		msg.Offset = entry.FromOffset

	case wal.Commit:
		msg.Type = message.CommitMessage

	default:
		msg.Type = message.UnknownMessage
	}

	msg.TupleID = message.NewTupleID(msg.Block, msg.Offset)

	return msg
}
