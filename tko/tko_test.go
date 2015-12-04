package tko

import (
	"testing"
	"time"

	"github.com/MediaMath/keryxlib/message"
)

func TestCheckClientStopsTrueCondition(t *testing.T) {
	success := CheckClient(getOutputStream(), trueCondition, nil)
	if !success {
		t.Fatal("expected success from check client")
	}
}

func TestCheckClientStopsOnTimeout(t *testing.T) {
	success := CheckClient(getOutputStream(), falseCondition, time.After(10*time.Millisecond))
	if success {
		t.Fatal("expected failure from check client")
	}
}

var trueCondition = BoolCondition(true)
var falseCondition = BoolCondition(false)

type BoolCondition bool

func (c BoolCondition) Check(*message.Transaction) bool {
	return bool(c)
}

func getOutputStream() chan *message.Transaction {
	out := make(chan *message.Transaction)

	go func() {
		for {
			messages := []message.Message{message.Message{}}
			out <- &message.Transaction{Messages: messages}
		}
	}()

	return out
}
