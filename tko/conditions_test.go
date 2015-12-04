package tko

import (
	"testing"

	"github.com/MediaMath/keryxlib/message"
)

func TestTransactionIdCondition(t *testing.T) {
	txn := &message.Transaction{TransactionID: 1234}
	matches(&TransactionIDMatches{1234}, txn, t, "TransactionID")
	doesntMatch(&TransactionIDMatches{12345}, txn, t, "TransactionID")
}

func TestHasMessageConditionMatchesAny(t *testing.T) {
	txn := &message.Transaction{Messages: []message.Message{message.Message{DatabaseName: "foo"}, message.Message{DatabaseName: "goo"}}}

	matches(&HasMessage{DatabaseName: p("goo")}, txn, t, "HasMessage matches any")
	doesntMatch(&HasMessage{DatabaseName: p("boo")}, txn, t, "HasMessage doesnt match if none")
}

func TestConditionDeserialize(t *testing.T) {
	_, err := ReadConditionFromJSON(`{"is_transaction": {"id": 1234}}`)
	if err != nil {
		t.Fatalf("Err: %v", err)
	}

	_, err = ReadConditionFromJSON(`{"has_message": {"database_name":"foo", "relation": "bar"}}`)
	if err != nil {
		t.Fatalf("Err: %v", err)
	}

	_, err = ReadConditionFromJSON(`{"has_message": {"missing_fields":true}}`)
	if err != nil {
		t.Fatalf("Err: %v", err)
	}
}

func p(str string) *string {
	return &str
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
