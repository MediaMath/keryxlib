package streams

import (
	"testing"

	"github.com/MediaMath/keryxlib/pg/wal"
)

func FailIfTrue(t *testing.T, val bool, message string) {
	if val {
		t.Fatal(message)
	}
}

func TestMessageFactoryCreateMessageUnknown(t *testing.T) {

	entry := wal.Entry{Type: wal.Unknown}

	message := createMessage(&entry)

	FailIfTrue(t, message.Type != 1, "message should be nil")
}

func TestMessageFactoryCreateMessageInsert(t *testing.T) {

	entry := wal.Entry{Type: wal.Insert}

	message := createMessage(&entry)

	FailIfTrue(t, message.Type != 2, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "InsertMessage", "MessageType.String() broken")

}

func TestMessageFactoryCreateMessageXLogDelete(t *testing.T) {

	entry := wal.Entry{Type: wal.Delete}

	message := createMessage(&entry)

	FailIfTrue(t, message.Type != 3, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "DeleteMessage", "MessageType.String() broken")
}

func TestMessageFactoryCreateMessageXLogUpdate(t *testing.T) {

	entry := wal.Entry{Type: wal.Update}

	message := createMessage(&entry)

	FailIfTrue(t, message.Type != 4, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "UpdateMessage", "MessageType.String() broken")
}

func TestMessageFactoryCreateMessageXLogCommit(t *testing.T) {

	entry := wal.Entry{Type: wal.Commit}

	message := createMessage(&entry)

	FailIfTrue(t, message.Type != 5, "Wrong message type")

	FailIfTrue(t, message.Type.String() != "CommitMessage", "MessageType.String() broken")
}
