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
	Filter          filters.MessageFilter
	SchemaReader    *pg.SchemaReader
	MaxMessageCount int
}

func interestingEntryType(entry *wal.Entry) bool {
	return entry.Type == wal.Insert || entry.Type == wal.Update || entry.Type == wal.Delete
}

func (b *PopulatedMessageStream) filterRelation(entry *wal.Entry) bool {
	return entry.RelationID > 0 && b.Filter.FilterRelID(entry.RelationID)
}

//Start begins async selecting on the WAL transaction buffer channel
func (b *PopulatedMessageStream) Start(serverVersion string, entryChan <-chan []*wal.Entry) (<-chan *message.Transaction, error) {
	txns := make(chan *message.Transaction)
	go func() {
		for entries := range entryChan {
			var messages []message.Message
			shouldPopulate := b.MaxMessageCount < 1 || len(entries) <= b.MaxMessageCount
			for _, entry := range entries {
				if interestingEntryType(entry) && b.SchemaReader.HaveConnectionToDb(entry.DatabaseID) && !b.filterRelation(entry) {
					msg := createMessage(entry)

					if shouldPopulate {
						msg.PopulateTime = time.Now().UTC()
						b.populate(msg)
						msg.PopulateDuration = time.Now().UTC().Sub(msg.PopulateTime)
					}

					messages = append(messages, *msg)
				}
			}

			if len(messages) > 0 {
				txn := &message.Transaction{}
				txn.Messages = messages
				txn.ServerVersion = serverVersion

				commit := entries[len(entries)-1]
				txn.TransactionID = commit.TransactionID
				txn.CommitKey = createKey(commit)
				txn.FirstKey = messages[0].Key
				txn.CommitTime = time.Unix(0, commit.ParseTime).UTC()

				if !shouldPopulate {
					txn.SwitchToTableBasedMessage()
				}

				txn.TransactionTime = time.Now().UTC()
				txns <- txn
			}
		}
		close(txns)
	}()

	return txns, nil
}

func (b *PopulatedMessageStream) waitForLogToCatchUp(rvMsg *message.Message) (curLoc uint64, lrl uint64, waits int) {

	curLoc = uint64(rvMsg.LogID)<<32 + uint64(rvMsg.RecordOffset)

	lrl = b.SchemaReader.LatestReplayLocation()
	for curLoc > lrl {
		<-time.After(time.Second)
		lrl = b.SchemaReader.LatestReplayLocation()
		waits++
	}

	return
}

func (b *PopulatedMessageStream) populate(rvMsg *message.Message) {
	curLoc, lrl, waits := b.waitForLogToCatchUp(rvMsg)
	rvMsg.PopulateWait = waits
	rvMsg.PopulateLag = lrl - curLoc

	if rvMsg.Type == message.InsertMessage || rvMsg.Type == message.UpdateMessage || rvMsg.Type == message.DeleteMessage {
		rvMsg.DatabaseName = b.SchemaReader.GetDatabaseName(rvMsg.DatabaseID)
		rvMsg.Namespace, rvMsg.Relation = b.SchemaReader.GetNamespaceAndTable(rvMsg.DatabaseID, rvMsg.RelationID)
	}

	if rvMsg.Type == message.InsertMessage || rvMsg.Type == message.UpdateMessage {

		vs, err := b.SchemaReader.GetFieldValues(rvMsg.DatabaseID, rvMsg.RelationID, rvMsg.Block, rvMsg.Offset)
		if err != nil {
			rvMsg.PopulationError = fmt.Sprintf("%v - (%v, %v, %v)", err.Error(), curLoc, lrl, waits)
		} else if vs == nil {
			rvMsg.PopulationError = fmt.Sprintf("Message skipped for no fields.")
		} else {
			for f, v := range vs {
				if !b.Filter.FilterColumn(rvMsg.RelFullName(), f.Column) {
					rvMsg.AppendField(f.Column, f.String(), v)
				}
			}
		}
	}

}

func createKey(entry *wal.Entry) message.Key {
	return message.NewKey(entry.TimelineID, entry.ReadFrom.LogID(), entry.ReadFrom.RecordOffset())
}

func createPrev(entry *wal.Entry) message.Key {
	return message.NewKey(entry.TimelineID, entry.Previous.LogID(), entry.Previous.RecordOffset())
}

func createMessage(entry *wal.Entry) *message.Message {
	msg := new(message.Message)

	msg.ParseTime = time.Unix(0, entry.ParseTime).UTC()
	msg.TimelineID = entry.TimelineID

	msg.LogID = entry.ReadFrom.LogID()
	msg.RecordOffset = entry.ReadFrom.RecordOffset()
	msg.Key = createKey(entry)
	msg.Prev = createPrev(entry)

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
		msg.PrevTupleID = message.NewTupleID(entry.FromBlock, entry.FromOffset)

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
