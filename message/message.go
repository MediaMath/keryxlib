package message

import (
	"encoding/json"
	"fmt"
)

const (
	keyStr   = "%.8X%.8X%.8X"
	tupleStr = "(%d,%d)"
)

type Field struct {
	Name  string `json:"n,omitempty"`
	Kind  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
}

type MessageType uint32

const (
	UnknownMessage MessageType = 1
	InsertMessage  MessageType = 2
	DeleteMessage  MessageType = 3
	UpdateMessage  MessageType = 4
	CommitMessage  MessageType = 5
	InfoMessage    MessageType = 6
	BatchMessage   MessageType = 7
)

func SerializedServerVersionMessage(version string) string {
	return fmt.Sprintf(`{"Type":%v,"ServerVersion":"%v"}`, InfoMessage, version)
}

func SerializedStatsMessage(connects, disconnects, messages uint64) string {
	return fmt.Sprintf(`{"Type":%v,"Connects":%v,"Disconnects":%v,"Active":%v,"Messages":%v}`,
		InfoMessage, connects, disconnects, connects-disconnects, messages)
}

func SerializedMessageBatch(messageWithKeyFound bool, messages []string) string {
	if messages == nil {
		messages = make([]string, 0)
	}
	batch := struct {
		Type                MessageType
		MessageWithKeyFound bool
		Messages            []string
	}{BatchMessage, messageWithKeyFound, messages}

	bs, err := json.Marshal(batch)
	if err == nil {
		return string(bs)
	}
	return ""
	// its ugly to use "" as an error code but the only errors that json marshalling can
	// encounter are around reflection and using a hardcoded struct should prevent that
}

func (messageType *MessageType) String() string {
	switch *messageType {
	case InsertMessage:
		return "InsertMessage"
	case DeleteMessage:
		return "DeleteMessage"
	case UpdateMessage:
		return "UpdateMessage"
	case CommitMessage:
		return "CommitMessage"
	case InfoMessage:
		return "InfoMessage"
	}

	return "UnknownMessage"
}

type MessageKey string

const EmptyMessageKey = MessageKey("")
const BeginningMessageKey = MessageKey("000000000000000000000000")

func Before(a MessageKey, b MessageKey) bool {
	return string(a) < string(b)
}

func NewKey(timelineId uint32, logId uint32, offset uint32) MessageKey {
	return MessageKey(fmt.Sprintf(keyStr, timelineId, logId, offset))
}

func KeyFromString(s string) MessageKey {
	return MessageKey(s)
}

func parseMessageKey(key MessageKey) (timelineId uint32, logId uint32, recordOffset uint32, err error) {
	keyString := string(key)
	if len(keyString) == 24 {
		_, err = fmt.Sscanf(keyString[:8], "%x", &timelineId)
		if err != nil {
			err = fmt.Errorf("error parsing key timeline id: %v", err)
		} else {
			_, err = fmt.Sscanf(keyString[8:16], "%x", &logId)
			if err != nil {
				err = fmt.Errorf("error parsing key log id: %v", err)
			} else {
				_, err = fmt.Sscanf(keyString[16:], "%x", &recordOffset)
				if err != nil {
					err = fmt.Errorf("error parsing key record offset: %v", err)
				}
			}
		}
	}

	return
}

type Message struct {
	TimelineId    uint32      `json:"-"`
	LogId         uint32      `json:"-"`
	RecordOffset  uint32      `json:"-"`
	TablespaceId  uint32      `json:"-"`
	DatabaseId    uint32      `json:"-"`
	RelationId    uint32      `json:"-"`
	Type          MessageType `json:"type"`
	Key           MessageKey  `json:"key"`
	Prev          MessageKey  `json:"prev"`
	TransactionId uint32      `json:"xid"`
	DatabaseName  string      `json:"db"`
	Namespace     string      `json:"ns"`
	Relation      string      `json:"rel"`
	Block         uint32      `json:"-"`
	Offset        uint16      `json:"-"`
	TupleId       string      `json:"ctid"`
	Fields        []Field     `json:"fields"`
	ServerVersion string      `json:",omitempty"`
}

func (msg *Message) String() string {
	if msg.Type == InfoMessage {
		return fmt.Sprintf("ServerVersion: %v", msg.ServerVersion)
	}
	return fmt.Sprintf("%v %.8X/%.8X/%.8X xid:%d %s.%s.%s (%d:%d)",
		msg.Type.String(), msg.TimelineId, msg.LogId, msg.RecordOffset, msg.TransactionId,
		msg.DatabaseName, msg.Namespace, msg.Relation, msg.Block, msg.Offset)
}

func (msg *Message) Serialize() ([]byte, error) {
	msg.UpdateKey()
	msg.UpdateTuple()
	return json.Marshal(msg)
}

func Unserialize(str string) (*Message, error) {
	msg := new(Message)

	err := json.Unmarshal([]byte(str), &msg)
	if err != nil {
		return nil, err
	}

	timeline, logid, offset, parseErr := parseMessageKey(msg.Key)
	if parseErr != nil {
		return nil, parseErr
	}

	msg.TimelineId = timeline
	msg.LogId = logid
	msg.RecordOffset = offset

	return msg, nil
}

func (msg *Message) UpdateKey() MessageKey {
	msg.Key = NewKey(msg.TimelineId, msg.LogId, msg.RecordOffset)
	return msg.Key
}

func (msg *Message) UpdateTuple() string {
	msg.TupleId = fmt.Sprintf(tupleStr, msg.Block, msg.Offset)
	return msg.TupleId
}

func (msg *Message) AppendField(name, kind, value string) {
	msg.Fields = append(msg.Fields, Field{name, kind, value})
}

func (msg *Message) LessThan(that *Message) bool {
	switch {
	case msg.TimelineId < that.TimelineId:
		return true
	case msg.TimelineId > that.TimelineId:
		return false
	}

	switch {
	case msg.LogId < that.LogId:
		return true
	case msg.LogId > that.LogId:
		return false
	}

	if msg.RecordOffset < that.RecordOffset {
		return true
	}

	return false
}
