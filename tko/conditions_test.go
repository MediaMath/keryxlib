package tko

import (
	"testing"

	"github.com/MediaMath/keryxlib/message"
)

func TestAlways(t *testing.T) {
	cond := condition(t, `{"always": {}}`)
	matches(cond, &message.Transaction{TransactionID: 1234}, t, "Always")
	matches(cond, &message.Transaction{TransactionID: 7878}, t, "Always")
}

func TestAnyOf(t *testing.T) {
	cond := condition(t, `{"any_of": [{"transaction_is": {"xid": 1234}}, {"transaction_is": {"xid":567}}]}`)
	matches(cond, &message.Transaction{TransactionID: 1234}, t, "AnyOf")
	matches(cond, &message.Transaction{TransactionID: 567}, t, "AnyOf")
	doesntMatch(cond, &message.Transaction{TransactionID: 12345}, t, "AnyOf")
}

func TestAllOf(t *testing.T) {
	cond := condition(t, `{"all_of": [{"transaction_is": {"xid": 1234}}, {"has_message": {"db":"foo"}}]}`)
	matches(cond, &message.Transaction{TransactionID: 1234, Messages: []message.Message{message.Message{DatabaseName: "foo"}}}, t, "AllOf")
	doesntMatch(cond, &message.Transaction{TransactionID: 1234}, t, "AllOf")
}

func TestNot(t *testing.T) {
	cond := condition(t, `{"not":{"transaction_is": {"xid": 1234}}}`)

	matches(cond, &message.Transaction{TransactionID: 12345}, t, "Not")
	doesntMatch(cond, &message.Transaction{TransactionID: 1234}, t, "Not")
}

func TestTransactionIs(t *testing.T) {
	cond := condition(t, `{"transaction_is": {"xid": 1234}}`)

	matches(cond, &message.Transaction{TransactionID: 1234}, t, "TransactionId")
	doesntMatch(cond, &message.Transaction{TransactionID: 12345}, t, "TransactionId")
}

func TestValidate(t *testing.T) {
	_, err := ReadConditionFromJSON(`{"not":{"transaction_is": {"id": 1234}}}`)
	if err == nil {
		t.Fatal("Should not have worked.")
	}
}

func TestHasMessage(t *testing.T) {
	cond := condition(t, `{"has_message": {"db": "foo"}}`)

	matches(cond, &message.Transaction{Messages: []message.Message{message.Message{DatabaseName: "foo"}, message.Message{DatabaseName: "goo"}}}, t, "HasMessage matches any")
	doesntMatch(cond, &message.Transaction{Messages: []message.Message{message.Message{DatabaseName: "boo"}, message.Message{DatabaseName: "goo"}}}, t, "HasMessage doesnt match if none")

	matches(cond, &message.Transaction{Tables: []message.Table{message.Table{DatabaseName: "goo"}, message.Table{DatabaseName: "foo"}}}, t, "Table style matches")
	doesntMatch(cond, &message.Transaction{Tables: []message.Table{message.Table{DatabaseName: "goo"}, message.Table{DatabaseName: "boo"}}}, t, "Table style doesnt match")
}

func TestHasMessageNamespace(t *testing.T) {
	cond := condition(t, `{"has_message": {"ns": "foo"}}`)

	matches(cond, &message.Transaction{Messages: []message.Message{message.Message{Namespace: "foo"}, message.Message{Namespace: "goo"}}}, t, "HasMessage matches any")
	doesntMatch(cond, &message.Transaction{Messages: []message.Message{message.Message{Namespace: "boo"}, message.Message{Namespace: "goo"}}}, t, "HasMessage doesnt match if none")

	matches(cond, &message.Transaction{Tables: []message.Table{message.Table{Namespace: "goo"}, message.Table{Namespace: "foo"}}}, t, "Table style matches")
	doesntMatch(cond, &message.Transaction{Tables: []message.Table{message.Table{Namespace: "goo"}, message.Table{Namespace: "boo"}}}, t, "Table style doesnt match")
}

func TestMissingFields(t *testing.T) {
	cond := condition(t, `{"has_message": {"missing_fields": true}}`)

	matches(cond, &message.Transaction{Messages: []message.Message{message.Message{DatabaseName: "foo"}}}, t, "MissingFields")
	fields := []message.Field{message.Field{Name: "boo"}}
	doesntMatch(cond, &message.Transaction{Messages: []message.Message{message.Message{Fields: fields}}}, t, "MissingFields")

	doesntMatch(cond, &message.Transaction{Tables: []message.Table{message.Table{Namespace: "goo"}, message.Table{Namespace: "boo"}}}, t, "Table style never matches missing fields")
}

func matches(condition Condition, txn *message.Transaction, t *testing.T, msg string) {
	if !condition.Check(txn) {
		t.Errorf("%v: didn't match", msg)
	}
}

func doesntMatch(condition Condition, txn *message.Transaction, t *testing.T, msg string) {
	if condition.Check(txn) {
		t.Errorf("%v: matched", msg)
	}
}

func condition(t *testing.T, json string) Condition {
	cond, err := ReadConditionFromJSON(json)
	if err != nil {
		t.Fatal(err)
	}

	err = cond.validate()
	if err != nil {
		t.Fatal(err)
	}

	return cond
}
