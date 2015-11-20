package message

import (
	"testing"
)

func FailIfTrue(t *testing.T, val bool, message string) {
	if val {
		t.Fatal(message)
	}
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
