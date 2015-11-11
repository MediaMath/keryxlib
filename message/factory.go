package message

import (
	"github.com/MediaMath/keryxlib/debug"
	"github.com/MediaMath/keryxlib/pg/wal"
)

type Populate func(*Message) (bool, error)
type Filter func(*wal.Entry) bool

func createMessage(entry *wal.Entry) (*Message, error) {
	msg := new(Message)

	msg.TimelineId = entry.TimelineID

	msg.LogId = entry.ReadFrom.LogID()
	msg.RecordOffset = entry.ReadFrom.RecordOffset()
	msg.Prev = NewKey(entry.TimelineID, entry.Previous.LogID(), entry.Previous.RecordOffset())

	msg.TransactionId = entry.TransactionID
	msg.TablespaceId = entry.TablespaceID
	msg.DatabaseId = entry.DatabaseID
	msg.RelationId = entry.RelationID

	msg.Fields = make([]Field, 0)

	switch entry.Type {
	case wal.Insert:
		msg.Type = InsertMessage
		msg.Block = entry.ToBlock
		msg.Offset = entry.ToOffset

	case wal.Update:
		msg.Type = UpdateMessage
		msg.Block = entry.ToBlock
		msg.Offset = entry.ToOffset

	case wal.Delete:
		msg.Type = DeleteMessage
		msg.Block = entry.FromBlock
		msg.Offset = entry.FromOffset

	case wal.Commit:
		msg.Type = CommitMessage

	default:
		return nil, nil
	}

	return msg, nil
}

func StartSendingMessages(d debug.Outputter, filter Filter, pop Populate, entryChan <-chan *wal.Entry, bufferWorkingDirectory string) (<-chan *Message, error) {
	publish := make(chan *Message)
	readValuesQueue := make(chan *Message)

	go populateAndPublish(pop, publish, readValuesQueue)

	go bufferMessageForWalEntry(d, filter, readValuesQueue, entryChan, bufferWorkingDirectory)

	return publish, nil
}

func populateAndPublish(pop Populate, publish, readValuesQueue chan *Message) {

	for rvMsg := range readValuesQueue {
		shouldPublish, _ := pop(rvMsg)

		if shouldPublish {
			publish <- rvMsg
		}
	}
	close(publish)
}

func bufferMessageForWalEntry(d debug.Outputter, filter Filter, readValuesQueue chan *Message, entryChan <-chan *wal.Entry, bufferWorkingDirectory string) {
	buffer := newBuffer(bufferWorkingDirectory, 10*1024*wal.EntryBytesSize, wal.EntryBytesSize)

	var lastEntry *wal.Entry

	publishTransaction := func(commitEntry *wal.Entry) {
		entriesBytes := buffer.remove(commitEntry.TransactionID)
		for _, entryBytes := range entriesBytes {
			entry := wal.EntryFromBytes(entryBytes)
			msg, err := createMessage(&entry)
			if err != nil {
				d("error while creating message: %v", err)
			} else if msg != nil {
				readValuesQueue <- msg
			}
		}

		commitMsg, err := createMessage(commitEntry)
		if err != nil {
			d("error while creating message: %v", err)
		} else if commitMsg != nil {
			readValuesQueue <- commitMsg
		}
	}

	for entry := range entryChan {
		if lastEntry != nil && lastEntry.ReadFrom.Offset() > entry.ReadFrom.Offset() {
			continue
		} else if entry.Type == wal.Unknown {
			continue
		} else if filter(entry) {
			continue
		}

		lastEntry = entry

		if entry.Type == wal.Commit {
			publishTransaction(entry)
		} else if entry.Type == wal.Abort {
			buffer.remove(entry.TransactionID)
		} else {
			buffer.add(entry.TransactionID, entry.ToBytes())
		}

	}
	close(readValuesQueue)
}
