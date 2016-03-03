package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import "testing"

func TestCorrectCountOfMultiInsertRecords(t *testing.T) {
	cptr, err := NewCursorAtPrevCheckpoint("./test_data/multi_insert")
	if err != nil {
		t.Fatalf("error creating cursor: %v", err)
	}
	c := *cptr

	const expectedMultiInserts = 7313

	actualMultiInserts := 0

	var ents []Entry

	for {
		ents, c, err = c.ReadEntries()
		if err == nil && ents != nil {
			for _, ent := range ents {
				if ent.Type == MultiInsert {
					actualMultiInserts++
				}
			}
		} else {
			break
		}
	}

	if actualMultiInserts != expectedMultiInserts {
		t.Errorf("incorrect count multi-inserts; expected %v but got %v", expectedMultiInserts, actualMultiInserts)
	}
}
