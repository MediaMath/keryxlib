package message

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"fmt"
	"strings"
	"time"
)

const (
	keyStr   = "%.8X%.8X%.8X"
	tupleStr = "(%d,%d)"
)

//Field is a column.
type Field struct {
	Name  string `json:"n,omitempty"`
	Kind  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
}

//Type is a mapping of the WAL record type.
type Type uint32

const (
	//UnknownMessage is an unsupported WAL record.
	UnknownMessage Type = 1
	//InsertMessage is an insert statement.
	InsertMessage Type = 2
	//DeleteMessage is a delete statement.
	DeleteMessage Type = 3
	//UpdateMessage is a update statement.
	UpdateMessage Type = 4
	//CommitMessage is a commit record.
	CommitMessage Type = 5
)

func (messageType *Type) String() string {
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

//NewTupleID creates a tuple string from the tuple data.
func NewTupleID(block uint32, offset uint16) string {
	return fmt.Sprintf(tupleStr, block, offset)
}

//Key is the LSN
type Key string

//EmptyKey represents a non-created key.
const EmptyKey = Key("")

//BeginningKey will always be before any other Key.
const BeginningKey = Key("000000000000000000000000")

//Before checks the keys to see which one came earlier in the WAL.
func Before(a Key, b Key) bool {
	return string(a) < string(b)
}

//NewKey creates a Key from the LSN
func NewKey(timelineID uint32, logID uint32, offset uint32) Key {
	return Key(fmt.Sprintf(keyStr, timelineID, logID, offset))
}

//KeyFromString creates a Key from a non-validated string.
func KeyFromString(s string) Key {
	return Key(s)
}

func parseMessageKey(key Key) (timelineID uint32, logID uint32, recordOffset uint32, err error) {
	keyString := string(key)
	if len(keyString) == 24 {
		_, err = fmt.Sscanf(keyString[:8], "%x", &timelineID)
		if err != nil {
			err = fmt.Errorf("error parsing key timeline id: %v", err)
		} else {
			_, err = fmt.Sscanf(keyString[8:16], "%x", &logID)
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

//Transaction is collection of messages all commited on the same postgres commit.
type Transaction struct {
	TransactionID   uint32    `json:"xid"`
	FirstKey        Key       `json:"first"`
	CommitKey       Key       `json:"commit"`
	CommitTime      time.Time `json:"commit_time"`
	TransactionTime time.Time `json:"transaction_time"`
	Messages        []Message `json:"messages,omitempty"`
	Tables          []Table   `json:"tables,omitempty"`
	MessageCount    int       `json:"message_count,omitempty"`
	ServerVersion   string    `json:"server_version,omitempty"`
}

//Table is the fully addressable form of a table
type Table struct {
	DatabaseName string `json:"db"`
	Namespace    string `json:"ns"`
	Relation     string `json:"rel"`
}

//RelFullName is a full table address of the form db.ns.table
func (msg Table) RelFullName() string {
	return fmt.Sprintf("%s.%s.%s", msg.DatabaseName, msg.Namespace, msg.Relation)
}

func TableFromFullName(fullName string) *Table {
	components := strings.Split(fullName, ".")
	if len(components) != 3 {
		return nil
	}

	return &Table{
		DatabaseName: components[0],
		Namespace:    components[1],
		Relation:     components[2],
	}
}

//SwitchToTableBasedMessage will get all the unique table names out of the messages
//add them to the tables field and empty the message field.  This is useful in contexts
//where the full transaction size would be too large.
func (t *Transaction) SwitchToTableBasedMessage() {
	//already table based
	if len(t.Tables) > 0 {
		return
	}

	m := make(map[string]Table)
	for _, msg := range t.Messages {
		m[msg.RelFullName()] = Table{DatabaseName: msg.DatabaseName, Namespace: msg.Namespace, Relation: msg.Relation}
	}

	var tables []Table
	for _, table := range m {
		tables = append(tables, table)
	}

	t.Tables = tables
	t.MessageCount = len(t.Messages)
	t.Messages = []Message{}
}

//Message is an individual populated commited postgres statement.
type Message struct {
	TimelineID       uint32        `json:"-"`
	LogID            uint32        `json:"-"`
	RecordOffset     uint32        `json:"-"`
	TablespaceID     uint32        `json:"nsid,omitempty"`
	DatabaseID       uint32        `json:"dbid,omitempty"`
	RelationID       uint32        `json:"relid,omitempty"`
	Type             Type          `json:"type"`
	Key              Key           `json:"key"`
	Prev             Key           `json:"prev"`
	TransactionID    uint32        `json:"xid"`
	DatabaseName     string        `json:"db"`
	Namespace        string        `json:"ns"`
	Relation         string        `json:"rel"`
	Block            uint32        `json:"-"`
	Offset           uint16        `json:"-"`
	TupleID          string        `json:"ctid"`
	PrevTupleID      string        `json:"prev_ctid,omitempty"`
	Fields           []Field       `json:"fields"`
	PopulationError  string        `json:"population_error,omitempty"`
	PopulateTime     time.Time     `json:"populate_time"`
	ParseTime        time.Time     `json:"parse_time"`
	PopulateWait     int           `json:"populate_wait,omitempty"`
	PopulateLag      uint64        `json:"lag,omitempty"`
	PopulateDuration time.Duration `json:"populate_duration,omitempty"`
}

//MissingFields returns true for any insert or update with no fields
func (msg *Message) MissingFields() bool {
	return msg.Type != DeleteMessage && len(msg.Fields) == 0
}

//RelFullName is a full table address of the form db.ns.table
func (msg *Message) RelFullName() string {
	return fmt.Sprintf("%s.%s.%s", msg.DatabaseName, msg.Namespace, msg.Relation)
}

func (msg *Message) String() string {
	return fmt.Sprintf("%v %.8X/%.8X/%.8X xid:%d %s.%s.%s (%d:%d)",
		msg.Type.String(), msg.TimelineID, msg.LogID, msg.RecordOffset, msg.TransactionID,
		msg.DatabaseName, msg.Namespace, msg.Relation, msg.Block, msg.Offset)
}

//AppendField adds a field to the message.
func (msg *Message) AppendField(name, kind, value string) {
	msg.Fields = append(msg.Fields, Field{name, kind, value})
}

//LessThan determines based on the LSN whether one message is before the other.
func (msg *Message) LessThan(that *Message) bool {
	switch {
	case msg.TimelineID < that.TimelineID:
		return true
	case msg.TimelineID > that.TimelineID:
		return false
	}

	switch {
	case msg.LogID < that.LogID:
		return true
	case msg.LogID > that.LogID:
		return false
	}

	if msg.RecordOffset < that.RecordOffset {
		return true
	}

	return false
}
