package tko

import (
	"encoding/json"
	"fmt"

	"github.com/MediaMath/keryxlib/message"
)

//ConditionDefinition wraps around the condition and allows it to be parsed generically.  Only 1 of the fields will be returned if multiple exist
type ConditionDefinition struct {
	IsTransaction *TransactionIDMatches  `json:"transaction_is"`
	HasMessage    *HasMessage            `json:"has_message"`
	Not           *ConditionDefinition   `json:"not"`
	AnyOf         *[]ConditionDefinition `json:"any_of"`
	AllOf         *[]ConditionDefinition `json:"all_of"`
}

func ReadConditionFromJSON(jsonStr string) (Condition, error) {
	var parsed ConditionDefinition
	err := json.Unmarshal([]byte(jsonStr), &parsed)

	if err != nil {
		return nil, err
	}

	return conditionFromDefinition(parsed)
}

func conditionFromDefinition(parsed ConditionDefinition) (Condition, error) {
	if parsed.IsTransaction != nil {
		return parsed.IsTransaction, nil
	}

	if parsed.HasMessage != nil {
		return parsed.HasMessage, nil
	}

	if parsed.Not != nil {
		cond, err := conditionFromDefinition(*parsed.Not)
		if err != nil {
			return nil, err
		}
		return &Not{cond}, nil
	}

	if parsed.AnyOf != nil {
		conditions := []Condition{}
		for _, definition := range *parsed.AnyOf {
			cond, err := conditionFromDefinition(definition)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		}

		return AnyOf(conditions), nil
	}

	if parsed.AllOf != nil {
		conditions := []Condition{}
		for _, definition := range *parsed.AllOf {
			cond, err := conditionFromDefinition(definition)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, cond)
		}

		return AllOf(conditions), nil
	}

	return nil, fmt.Errorf("Unknown condition: %v", parsed)
}

//Not will return the inverse of the underlying condition.
type Not struct {
	condition Condition
}

func (c *Not) Check(txn *message.Transaction) bool {
	orig := c.condition.Check(txn)
	return !orig
}

//AnyOf will return true if any of its underlying Conditions return true. Logical Or.
type AnyOf []Condition

func (c AnyOf) Check(txn *message.Transaction) bool {
	if len(c) == 0 {
		return false
	}

	for _, condition := range c {
		if condition.Check(txn) {
			return true
		}
	}

	return false
}

//AllOf will return true if all of its underlying Conditions return true. Logical And.
type AllOf []Condition

func (c AllOf) Check(txn *message.Transaction) bool {
	if len(c) == 0 {
		return true
	}

	for _, condition := range c {
		if !condition.Check(txn) {
			return false
		}
	}

	return true
}

//TransactionIDMatches will match a specific TransactionID
type TransactionIDMatches struct {
	TransactionID uint32 `json:"xid"`
}

func (c *TransactionIDMatches) Check(txn *message.Transaction) bool {
	return c.TransactionID == txn.TransactionID
}

//HasMessage will match if any message in the transaction matches the provided message.
type HasMessage struct {
	Type          *message.Type          `json:"type"`
	DatabaseName  *string                `json:"db"`
	Namespace     *string                `json:"ns`
	Relation      *string                `json:"rel"`
	TupleID       *string                `json:"ctid"`
	PrevTupleID   *string                `json:"prev_ctid"`
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
