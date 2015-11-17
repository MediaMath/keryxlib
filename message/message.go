package message

import "fmt"

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
)

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
	}

	return "UnknownMessage"
}

func NewTupleId(block uint32, offset uint16) string {
	return fmt.Sprintf(tupleStr, block, offset)
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

type Transaction struct {
	TransactionId uint32     `json:"xid"`
	FirstKey      MessageKey `json:"first"`
	CommitKey     MessageKey `json:"commit"`
	Messages      []Message  `json:"messages"`
}

type Message struct {
	TimelineId      uint32      `json:"-"`
	LogId           uint32      `json:"-"`
	RecordOffset    uint32      `json:"-"`
	TablespaceId    uint32      `json:"-"`
	DatabaseId      uint32      `json:"-"`
	RelationId      uint32      `json:"-"`
	Type            MessageType `json:"type"`
	Key             MessageKey  `json:"key"`
	Prev            MessageKey  `json:"prev"`
	TransactionId   uint32      `json:"xid"`
	DatabaseName    string      `json:"db"`
	Namespace       string      `json:"ns"`
	Relation        string      `json:"rel"`
	Block           uint32      `json:"-"`
	Offset          uint16      `json:"-"`
	TupleId         string      `json:"ctid"`
	Fields          []Field     `json:"fields"`
	ServerVersion   string      `json:",omitempty"`
	PopulationError error       `json:"population_error"`
}

func (msg *Message) RelFullName() string {
	return fmt.Sprintf("%s.%s.%s", msg.DatabaseName, msg.Namespace, msg.Relation)
}

func (msg *Message) String() string {
	return fmt.Sprintf("%v %.8X/%.8X/%.8X xid:%d %s.%s.%s (%d:%d)",
		msg.Type.String(), msg.TimelineId, msg.LogId, msg.RecordOffset, msg.TransactionId,
		msg.DatabaseName, msg.Namespace, msg.Relation, msg.Block, msg.Offset)
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
