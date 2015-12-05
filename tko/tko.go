package tko

import "github.com/MediaMath/keryxlib/message"

// Condition models the an assertion that can be applied to a Transaction
type Condition interface {
	Check(*message.Transaction) bool
	validate() error
}
