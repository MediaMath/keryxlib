package tko

import (
	"fmt"
	"strings"
	"testing"

	"github.com/MediaMath/keryxlib/message"
)

func failIfError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Error: %v", err)
	}
}

func failIfTrue(t *testing.T, val bool, message string) {
	if val {
		t.Fatal(message)
	}
}

func TestCommit(t *testing.T) {
	t.Skip("Need to rework for transactions")
	testConditionMatches(t, combineConditions(exampleCommit), matchingCommit)
}

func TestInsert(t *testing.T) {
	testConditionMatches(t, combineConditions(exampleInsert), matchingInsert)
}

func TestUpdate(t *testing.T) {
	testConditionMatches(t, combineConditions(exampleUpdate), matchingUpdate)
}

func TestDelete(t *testing.T) {
	testConditionMatches(t, combineConditions(exampleDelete), matchingDelete)
}

func TestMultiple(t *testing.T) {
	c, err := ReadConditionFromJSON(combineConditions(exampleInsert, exampleUpdate))
	failIfError(t, err)
	failIfTrue(t, c.Check(nonMatching), "shouldn't have matched")
	failIfTrue(t, c.Check(matchingUpdate), "shouldn't have matched yet")
	failIfTrue(t, !c.Check(matchingInsert), "should have matched")
}

func testConditionMatches(t *testing.T, condStr string, shouldMatch message.Message) {
	c, err := ReadConditionFromJSON(condStr)
	failIfError(t, err)
	failIfTrue(t, c.Check(nonMatching), "shouldn't have matched")
	failIfTrue(t, !c.Check(shouldMatch), "should have matched")
}

func combineConditions(condStrs ...string) string {
	return fmt.Sprintf("{\"Conditions\":[%v]}", strings.Join(condStrs, ","))
}

var (
	exampleCommit = `{
	"Type": "Commit",
	"TransactionId": 1234
}`
	exampleInsert = `{
	"Type": "Insert",
  "DatabaseName": "postgres",
  "Namespace": "public",
  "Relation": "test",
  "FieldsMatch": {
  	"msg": "insert"
  }
}`
	exampleUpdate = `{
	"Type": "Update",
  "DatabaseName": "postgres",
  "Namespace": "public",
  "Relation": "test",
  "FieldsMatch": {
  	"msg": "update"
  }
}`
	exampleDelete = `{
	"Type": "Delete",
  "DatabaseName": "postgres",
  "Namespace": "public",
  "Relation": "test"
}`
	matchingCommit = message.Message{
		Type: message.CommitMessage,
	}
	matchingInsert = message.Message{
		Type:         message.InsertMessage,
		DatabaseName: "postgres",
		Namespace:    "public",
		Relation:     "test",
		Fields: []message.Field{
			message.Field{Name: "otherfield1", Value: "noise1"},
			message.Field{Name: "msg", Value: "insert"},
			message.Field{Name: "otherfield2", Value: "noise2"},
		},
	}
	matchingUpdate = message.Message{
		Type:         message.UpdateMessage,
		DatabaseName: "postgres",
		Namespace:    "public",
		Relation:     "test",
		Fields: []message.Field{
			message.Field{Name: "otherfield1", Value: "noise1"},
			message.Field{Name: "msg", Value: "update"},
			message.Field{Name: "otherfield2", Value: "noise2"},
		},
	}
	matchingDelete = message.Message{
		Type:         message.DeleteMessage,
		DatabaseName: "postgres",
		Namespace:    "public",
		Relation:     "test",
	}
	nonMatching = message.Message{
		Type:         message.InsertMessage,
		DatabaseName: "xxxx",
		Namespace:    "yyyy",
		Relation:     "zzzz",
		Fields: []message.Field{
			message.Field{Name: "otherfield1", Value: "noise1"},
			message.Field{Name: "otherfield2", Value: "noise2"},
		},
	}
)
