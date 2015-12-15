package tko

import (
	"encoding/json"
	"fmt"

	"github.com/MediaMath/keryxlib/message"
)

//ConditionDefinition wraps around the condition and allows it to be parsed generically.  Only 1 of the fields will be returned if multiple exist
type ConditionDefinition struct {
	Always        *Always                `json:"always"`
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

func conditionFromDefinition(parsed ConditionDefinition) (cond Condition, err error) {
	if parsed.IsTransaction != nil {
		cond = parsed.IsTransaction
	} else if parsed.Always != nil {
		cond = parsed.Always
	} else if parsed.HasMessage != nil {
		cond = parsed.HasMessage
	} else if parsed.Not != nil {
		var inner Condition
		inner, err = conditionFromDefinition(*parsed.Not)
		if err == nil {
			cond = &Not{inner}
		}
	} else if parsed.AnyOf != nil {
		cond, err = AnyOfThese(*parsed.AnyOf)
	} else if parsed.AllOf != nil {
		cond, err = AllOfThese(*parsed.AllOf)
	} else {
		err = fmt.Errorf("Unknown condition: %v", parsed)
	}

	if cond != nil {
		err = cond.validate()
	}

	return
}

func definitionsToConditions(definitions []ConditionDefinition) ([]Condition, error) {
	conditions := []Condition{}
	for _, definition := range definitions {
		cond, err := conditionFromDefinition(definition)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}

	return conditions, nil
}

//Always will always return true
type Always struct{}

func (c *Always) Check(txn *message.Transaction) bool {
	return true
}

func (c *Always) validate() error {
	return nil
}

//Not will return the inverse of the underlying condition.
type Not struct {
	condition Condition
}

func (c *Not) Check(txn *message.Transaction) bool {
	orig := c.condition.Check(txn)
	return !orig
}

func (c *Not) validate() error {
	if c.condition == nil {
		return fmt.Errorf("No condition to invert.")
	}

	return nil
}

//AnyOf will return true if any of its underlying Conditions return true. Logical Or.
type AnyOf []Condition

//AnyOfThese creates an AnyOf from other definitions
func AnyOfThese(definitions []ConditionDefinition) (anyOf AnyOf, err error) {
	conditions, err := definitionsToConditions(definitions)
	if err == nil {
		anyOf = AnyOf(conditions)
	}

	return
}

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

func (c AnyOf) validate() error {
	if len(c) < 1 {
		return fmt.Errorf("Must have conditions for AnyOf")
	}

	return nil
}

//AllOf will return true if all of its underlying Conditions return true. Logical And.
type AllOf []Condition

//AllOfThese creates an AllOf Condition from a list of definitions
func AllOfThese(definitions []ConditionDefinition) (allOf AllOf, err error) {
	conditions, err := definitionsToConditions(definitions)
	if err == nil {
		allOf = AllOf(conditions)
	}

	return
}

func (c AllOf) Check(txn *message.Transaction) bool {
	for _, condition := range c {
		if !condition.Check(txn) {
			return false
		}
	}

	return true
}

func (c AllOf) validate() error {
	if len(c) < 1 {
		return fmt.Errorf("Must have conditions for AllOf")
	}

	return nil
}

//TransactionIDMatches will match a specific TransactionID
type TransactionIDMatches struct {
	TransactionID uint32 `json:"xid"`
}

func (c *TransactionIDMatches) Check(txn *message.Transaction) bool {
	return c.TransactionID == txn.TransactionID
}

func (c *TransactionIDMatches) validate() error {
	if c.TransactionID < 1 {
		return fmt.Errorf("TransactionID is missing or not positive: %v", c.TransactionID)
	}

	return nil
}

//HasMessage will match if any message in the transaction matches the provided message.
type HasMessage struct {
	Type          *message.Type          `json:"type"`
	DatabaseName  *string                `json:"db"`
	Namespace     *string                `json:"ns"`
	Relation      *string                `json:"rel"`
	TupleID       *string                `json:"ctid"`
	PrevTupleID   *string                `json:"prev_ctid"`
	FieldsMatch   map[string]interface{} `json:"fields_match"`
	MissingFields *bool                  `json:"missing_fields"`
	Waits         *bool                  `json:"waits"`
}

func (c *HasMessage) Check(txn *message.Transaction) bool {
	for _, msg := range txn.Messages {
		if c.checkMessage(msg) {
			return true
		}
	}

	return false
}

func (c *HasMessage) validate() error {
	if c.Waits == nil && c.Type == nil && c.DatabaseName == nil && c.Namespace == nil && c.Relation == nil && c.TupleID == nil && c.PrevTupleID == nil && len(c.FieldsMatch) == 0 && c.MissingFields == nil {
		return fmt.Errorf("No message conditions specified.")
	}

	return nil
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
		chk.waits() &&
		chk.fieldsMatch()
}

type checkable struct {
	cond *HasMessage
	msg  message.Message
}

func (c checkable) waits() bool {
	return c.cond.Waits == nil || c.msg.PopulateWait > 0 == *c.cond.Waits
}

func (c checkable) missingFields() bool {
	return c.cond.MissingFields == nil || c.msg.MissingFields() == *c.cond.MissingFields
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
