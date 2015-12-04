package tko

import (
	"encoding/json"
	"fmt"

	"github.com/MediaMath/keryxlib/message"
)

//ConditionDefinition wraps around the condition and allows it to be parsed generically.  Only 1 of the fields will be returned if multiple exist
type ConditionDefinition struct {
	IsTransaction *TransactionIDMatches `json:"is_transaction"`
	HasMessage    *HasMessage           `json:"has_message"`
}

func ReadConditionFromJSON(jsonStr string) (Condition, error) {
	var parsed ConditionDefinition
	err := json.Unmarshal([]byte(jsonStr), &parsed)

	if err != nil {
		return nil, err
	}

	if parsed.IsTransaction != nil {
		return parsed.IsTransaction, nil
	}

	if parsed.HasMessage != nil {
		return parsed.HasMessage, nil
	}

	return nil, fmt.Errorf("Unknown condition: %v %s", parsed, jsonStr)
}

//TransactionIDMatches will match a specific TransactionID
type TransactionIDMatches struct {
	TransactionID uint32 `json:"id"`
}

func (c *TransactionIDMatches) Check(txn *message.Transaction) bool {
	return c.TransactionID == txn.TransactionID
}

//HasMessage will match if any message in the transaction matches the provided message.
type HasMessage struct {
	Type          *message.Type          `json:"type"`
	DatabaseName  *string                `json:"database_name"`
	Namespace     *string                `json:"namespace"`
	Relation      *string                `json:"relation"`
	TupleID       *string                `json:"tuple_id"`
	PrevTupleID   *string                `json:"prev_tuple_id"`
	FieldsMatch   map[string]interface{} `json:"fields_match"`
	MissingFields *bool                  `json:"missing_fields"`
}

func (c *HasMessage) Check(txn *message.Transaction) bool {
	for _, msg := range txn.Messages {
		if c.checkMessage(msg) {
			return true
		}
	}

	return false
}

func (c *HasMessage) checkMessage(m message.Message) bool {
	chk := checkable{c, m}
	return chk.typeMatches() &&
		chk.databaseNameMatches() &&
		chk.namespaceMatches() &&
		chk.relationMatches() &&
		chk.tupleIDMatches() &&
		chk.prevTupleIDMatches() &&
		chk.missingFields() &&
		chk.fieldsMatch()
}

type checkable struct {
	cond *HasMessage
	msg  message.Message
}

func (c checkable) missingFields() bool {
	return c.cond.MissingFields == nil || (c.msg.Type != message.DeleteMessage && (len(c.msg.Fields) == 0) == *c.cond.MissingFields)
}

func (c checkable) typeMatches() bool {
	return c.cond.Type == nil || c.msg.Type == *c.cond.Type
}

func (c checkable) databaseNameMatches() bool {
	return c.cond.DatabaseName == nil || c.msg.DatabaseName == *c.cond.DatabaseName
}

func (c checkable) namespaceMatches() bool {
	return c.cond.Namespace == nil || c.msg.Namespace == *c.cond.Namespace
}

func (c checkable) relationMatches() bool {
	return c.cond.Relation == nil || c.msg.Relation == *c.cond.Relation
}

func (c checkable) tupleIDMatches() bool {
	return c.cond.TupleID == nil || c.msg.TupleID == *c.cond.TupleID
}

func (c checkable) prevTupleIDMatches() bool {
	return c.cond.PrevTupleID == nil || c.msg.PrevTupleID == *c.cond.PrevTupleID
}

func (c checkable) fieldsMatch() bool {
	msgFieldsContains := makeFieldsIndex(c.msg.Fields)

	for n, v := range c.cond.FieldsMatch {
		if !msgFieldsContains(n, v) {
			return false
		}
	}

	return true
}

func makeFieldsIndex(fields []message.Field) func(string, interface{}) bool {
	index := make(map[string]interface{})

	for _, f := range fields {
		index[fmt.Sprintf("%v=%v", f.Name, f.Value)] = true
	}

	return func(name string, value interface{}) bool {
		if _, ok := index[fmt.Sprintf("%v=%v", name, value)]; ok {
			return true
		}

		return false
	}
}
