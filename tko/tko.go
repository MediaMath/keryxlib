package tko

import (
	"time"

	"github.com/MediaMath/keryxlib/message"
)

// Condition models the an assertion that can be applied to a Transaction
type Condition interface {
	Check(*message.Transaction) bool
}

// CheckClient tests a condition against a client output stream
func CheckClient(stream chan *message.Transaction, condition Condition, timeout <-chan time.Time) bool {

checkLoop:
	for {
		select {
		case <-timeout:
			break checkLoop
		case txn, ok := <-stream:
			if ok {
				if condition.Check(txn) {
					return true
				}
			} else {
				break checkLoop
			}
		}
	}

	return false
}
