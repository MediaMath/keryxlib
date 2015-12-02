package tko

import (
	"encoding/json"
	"fmt"

	"github.com/MediaMath/keryxlib/message"
)

// ReadConditionFromJSON will produce a condition from a json string
func ReadConditionFromJSON(jsonStr string) (Condition, error) {
	var jc jsonConditions

	if err := json.Unmarshal([]byte(jsonStr), &jc); err != nil {
		return nil, err
	}

	return &jc, nil
}

type jsonConditions struct {
	Conditions []jsonCondition
	Invert     bool
}

func (cs *jsonConditions) Check(mw message.Message) bool {
	var out []jsonCondition
	result := false

	for _, c := range cs.Conditions {
		if !c.Check(mw) {
			out = append(out, c)
		}
	}

	cs.Conditions = out

	if len(cs.Conditions) == 0 {
		result = true
	}

	if cs.Invert {
		result = !result
	}

	return result
}

type jsonCondition struct {
	Type          *string                `json:"Type"`
	TransactionID *uint32                `json:"TransactionId"`
	DatabaseName  *string                `json:"DatabaseName"`
	Namespace     *string                `json:"Namespace"`
	Relation      *string                `json:"Relation"`
	TupleID       *string                `json:"TupleId"`
	FieldsMatch   map[string]interface{} `json:"FieldsMatch"`
}

func (c *jsonCondition) Check(mw message.Message) bool {
	if c.Type != nil {
		switch *c.Type {
		case "Commit":
			return c.checkCommit(mw)
		case "Insert":
			fallthrough
		case "Update":
			return c.checkInsertUpdate(mw)
		case "Delete":
			return c.checkDelete(mw)
		}
	}

	return false
}

func (c *jsonCondition) checkCommit(m message.Message) bool {
	chk := checkable{c, m}
	return chk.transactionIDMatches()
}

func (c *jsonCondition) checkInsertUpdate(m message.Message) bool {
	chk := checkable{c, m}
	return chk.transactionIDMatches() &&
		chk.databaseNameMatches() &&
		chk.namespaceMatches() &&
		chk.relationMatches() &&
		chk.tupleIDMatches() &&
		chk.fieldsMatch()
}

func (c *jsonCondition) checkDelete(m message.Message) bool {
	chk := checkable{c, m}
	return chk.transactionIDMatches() &&
		chk.databaseNameMatches() &&
		chk.namespaceMatches() &&
		chk.relationMatches() &&
		chk.tupleIDMatches()
}

type checkable struct {
	cond *jsonCondition
	msg  message.Message
}

func (c checkable) transactionIDMatches() bool {
	return true
	//need to rework for transaction based
	//return c.cond.TransactionID == nil || c.msg.TransactionId == *c.cond.TransactionID
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
