package tko

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"encoding/json"
	"fmt"

	"github.com/MediaMath/keryxlib/message"
)

//ConditionDefinition wraps around the condition and allows it to be parsed generically.  Only 1 of the fields will be returned if multiple exist
type ConditionDefinition struct {
	Always           *Always                `json:"always"`
	IsTransaction    *TransactionIs         `json:"transaction_is"`
	TransactionsThat *[]ConditionDefinition `json:"transactions_that"`
	HasMessage       *HasMessage            `json:"has_message"`
	Not              *ConditionDefinition   `json:"not"`
	AnyOf            *[]ConditionDefinition `json:"any_of"`
	AllOf            *[]ConditionDefinition `json:"all_of"`
}

//ReadConditionFromJSON parses a json string into a condition
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
	} else if parsed.TransactionsThat != nil {
		cond, err = TransactionsOfThese(*parsed.TransactionsThat)
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

//Check will always return true for an always condition.
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

//Check will return the opposite of the underlying condition for the not condition.
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

//TransactionsThat will return false until a transaction is found for every conditions.  It is a stateful transaction filter.
type TransactionsThat struct {
	conditions []Condition
}

//TransactionsOfThese creates an TransactionsThat from other definitions
func TransactionsOfThese(definitions []ConditionDefinition) (transactionsThat *TransactionsThat, err error) {
	conditions, err := definitionsToConditions(definitions)
	if err == nil {
		transactionsThat = &TransactionsThat{conditions}
	}

	return
}

//Check will return true if any of the underlying conditions return true.
func (t *TransactionsThat) Check(txn *message.Transaction) bool {

	var keep []Condition
	for _, condition := range t.conditions {
		if !condition.Check(txn) {
			keep = append(keep, condition)
		}
	}
	t.conditions = keep

	return len(t.conditions) == 0
}

func (c *TransactionsThat) validate() error {
	if len(c.conditions) < 1 {
		return fmt.Errorf("Must have conditions for TransactionsThat")
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

//Check will return true if any of the underlying conditions return true.
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

//Check will return true if all of the underlying conditions return true.
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

//TransactionIs will match a specific TransactionID
type TransactionIs struct {
	TransactionID  *uint32 `json:"xid"`
	BigTransaction *bool   `json:"big"`
}

//Check will return true if the transaction id matches for TransactionIDMatches conditions.
func (c *TransactionIs) Check(txn *message.Transaction) bool {
	return (c.TransactionID == nil || *c.TransactionID == txn.TransactionID) &&
		(c.BigTransaction == nil || *c.BigTransaction == (len(txn.Tables) > 0))
}

func (c *TransactionIs) validate() error {
	if c.TransactionID != nil && *c.TransactionID < 1 {
		return fmt.Errorf("TransactionID is missing or not positive: %v", c.TransactionID)
	}

	if c.TransactionID == nil && c.BigTransaction == nil {
		return fmt.Errorf("No condition")
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

//Check will return true if all of the defined conditions for HasMessage match.
func (c *HasMessage) Check(txn *message.Transaction) bool {
	for _, msg := range txn.Messages {
		if c.checkMessage(msg) {
			return true
		}
	}

	for _, tables := range txn.Tables {
		if c.checkTable(tables) {
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

func (c *HasMessage) checkTable(t message.Table) bool {
	chk := tableCheck{c, t}
	return chk.cond.Type == nil &&
		chk.cond.TupleID == nil &&
		chk.cond.PrevTupleID == nil &&
		chk.cond.MissingFields == nil &&
		chk.cond.Waits == nil &&
		chk.cond.FieldsMatch == nil &&
		chk.databaseNameMatches() &&
		chk.namespaceMatches() &&
		chk.relationMatches()
}

type tableCheck struct {
	cond *HasMessage
	t    message.Table
}

func (c tableCheck) databaseNameMatches() bool {
	return c.cond.DatabaseName == nil || c.t.DatabaseName == *c.cond.DatabaseName
}

func (c tableCheck) namespaceMatches() bool {
	return c.cond.Namespace == nil || c.t.Namespace == *c.cond.Namespace
}

func (c tableCheck) relationMatches() bool {
	return c.cond.Relation == nil || c.t.Relation == *c.cond.Relation
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
