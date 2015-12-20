package streams

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"testing"
	"time"

	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/pg/wal"
)

func TestBufferMessageForCommit(t *testing.T) {

	now := time.Now()
	updateEntry := &wal.Entry{Type: wal.Update, TransactionID: 10, ParseTime: now.UnixNano()}
	updateNeverCommitted := &wal.Entry{Type: wal.Update, TransactionID: 1}
	commitEntry := &wal.Entry{Type: wal.Commit, TransactionID: 10}

	walLog := make(chan *wal.Entry)

	go func() {
		walLog <- updateEntry
		walLog <- updateNeverCommitted
		walLog <- commitEntry
	}()

	buffer := &TxnBuffer{filters.FilterNone("buffer"), "."}
	txns, err := buffer.Start(walLog)
	if err != nil {
		t.Fatal(err)
	}

	txn := <-txns
	if txn == nil {
		t.Fatal("Should not be nil")
	}
	FailIfTrue(t, len(txn) != 2, "Txn List not right")
	FailIfTrue(t, txn[0].Type != wal.Update, "Not matching value")
	FailIfTrue(t, txn[0].ParseTime != now.UnixNano(), "Not matching value")
	FailIfTrue(t, txn[1].Type != wal.Commit, "Not matching value")
}
