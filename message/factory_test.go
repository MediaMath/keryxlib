package message

import (
	"fmt"
	"testing"

	"github.com/MediaMath/keryxlib/debug"
	"github.com/MediaMath/keryxlib/pg/wal"
)

func FailIfTrue(t *testing.T, val bool, message string) {
	if val {
		t.Fatal(message)
	}
}

func TestMessageFactoryCreateMessageUnknown(t *testing.T) {

	entry := wal.Entry{Type: wal.Unknown}

	message, _ := createMessage(&entry)

	FailIfTrue(t, message != nil, "message should be nil")
}

func TestMessageFactoryCreateMessageInsert(t *testing.T) {

	entry := wal.Entry{Type: wal.Insert}

	message, _ := createMessage(&entry)

	FailIfTrue(t, message.Type != 2, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "InsertMessage", "MessageType.String() broken")

}

func TestMessageFactoryCreateMessageXLogDelete(t *testing.T) {

	entry := wal.Entry{Type: wal.Delete}

	message, _ := createMessage(&entry)

	FailIfTrue(t, message.Type != 3, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "DeleteMessage", "MessageType.String() broken")
}

func TestMessageFactoryCreateMessageXLogUpdate(t *testing.T) {

	entry := wal.Entry{Type: wal.Update}

	message, _ := createMessage(&entry)

	FailIfTrue(t, message.Type != 4, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "UpdateMessage", "MessageType.String() broken")
}

func TestMessageFactoryCreateMessageXLogCommit(t *testing.T) {

	entry := wal.Entry{Type: wal.Commit}

	message, _ := createMessage(&entry)

	FailIfTrue(t, message.Type != 5, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "CommitMessage", "MessageType.String() broken")
}

func TestMessageStringOutputInsert(t *testing.T) {

	message := Message{Type: InsertMessage, Offset: 3}

	FailIfTrue(t, message.String() != "InsertMessage 00000000/00000000/00000000 xid:0 .. (0:3)", "String() broken")
}

func TestMessageStringOutputDelete(t *testing.T) {

	message := Message{Type: DeleteMessage, Offset: 3}

	FailIfTrue(t, message.String() != "DeleteMessage 00000000/00000000/00000000 xid:0 .. (0:3)", "String() broken")
}

func TestMessageStringOutputUpdate(t *testing.T) {

	message := Message{Type: UpdateMessage, Offset: 1}

	FailIfTrue(t, message.String() != "UpdateMessage 00000000/00000000/00000000 xid:0 .. (0:1)", "String() broken")
}

func TestMessageStringOutputCommit(t *testing.T) {

	message := Message{Type: CommitMessage}

	FailIfTrue(t, message.String() != "CommitMessage 00000000/00000000/00000000 xid:0 .. (0:0)", "String() broken")
}

func TestMessageSerialize(t *testing.T) {

	message := Message{Type: CommitMessage}

	bytes, err := message.Serialize()

	if err != nil || bytes == nil {
		t.Fatal("bytes should not be nil")
	}
}

func TestMessageUpdateKey(t *testing.T) {

	message := Message{}

	key := message.UpdateKey()

	FailIfTrue(t, key != "000000000000000000000000", "UpdateKey broken")
}

func TestMessageAppendField(t *testing.T) {

	message := &Message{}

	FailIfTrue(t, len(message.Fields) > 0, "fields should contain nothing at this moment")

	//add stuff
	message.AppendField("a", "a", "a")
	message.AppendField("b", "b", "b")
	message.AppendField("c", "c", "c")

	FailIfTrue(t, len(message.Fields) != 3, "fields should contain 3 things at this moment")

	//make sure it added
	FailIfTrue(t, checkMessageFields(message, 0, "a"), "AppendField is broken")

	FailIfTrue(t, checkMessageFields(message, 1, "b"), "AppendField is broken")

	FailIfTrue(t, checkMessageFields(message, 2, "c"), "AppendField is broken")

}

func checkMessageFields(message *Message, index int, expectedValue string) bool {

	for i, v := range message.Fields {
		if i == index {
			return v.Name != expectedValue && v.Kind != expectedValue && v.Value != expectedValue
		}
	}

	return message.Fields[index].Name != expectedValue
}

//type Populate func(*Message) (bool, error)
func mockPopulate(msg *Message) (bool, error) {
	if msg != nil {
		msg.DatabaseId = 1
		msg.RelationId = 12
		msg.DatabaseName = "postgres"
	}

	return true, nil
}

func mockPopulateFail(msg *Message) (bool, error) {
	return false, fmt.Errorf("mockPopulateFail")
}

//This test isn't great, mostly because it requireds DB
func TestPopulateAndPublish(t *testing.T) {

	publish := make(chan *Message)
	readValuesQueue := make(chan *Message, 1000000)

	readValuesQueue <- &Message{}

	go func() {
		for val := range publish {
			if val == nil {
				t.Fatal("Should not be nil")
			}
		}
	}()

	go populateAndPublish(mockPopulate, publish, readValuesQueue)
}

func TestFailurePopulate(t *testing.T) {

	publish := make(chan *Message)
	readValuesQueue := make(chan *Message, 1000000)

	go func() { readValuesQueue <- &Message{} }()

	go populateAndPublish(mockPopulateFail, publish, readValuesQueue)
}

func TestBufferMessageForCommit(t *testing.T) {

	updateEntry := &wal.Entry{Type: wal.Update, TransactionID: 10}
	updateNeverCommitted := &wal.Entry{Type: wal.Update, TransactionID: 1}
	commitEntry := &wal.Entry{Type: wal.Commit, TransactionID: 10}

	outChan := make(chan *wal.Entry)
	readValuesQueue := make(chan *Message, 1000000)

	go func() {
		outChan <- updateEntry
		outChan <- updateNeverCommitted
		outChan <- commitEntry
	}()

	go bufferMessageForWalEntry(debug.NullOutputter, func(ent *wal.Entry) bool { return false }, readValuesQueue, outChan, ".")

	val := <-readValuesQueue
	if val == nil {
		t.Fatal("Should not be nil")
	}
	FailIfTrue(t, val.Type != UpdateMessage, "Not matching value")

	val = <-readValuesQueue
	if val == nil {
		t.Fatal("Should not be nil")
	}
	FailIfTrue(t, val.Type != CommitMessage, "Not matching value")
}
