package tko

import "github.com/MediaMath/keryxlib/message"

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Condition models the an assertion that can be applied to a Transaction
type Condition interface {
	Check(*message.Transaction) bool
	validate() error
}
