package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"os"
	"testing"
)

const testDataDir = "./test_data/data"
const prepareTestDataDir = "./test_data/prepare_test_data_dir.sh"

func TestCorrectCountOfRecordsParsedSincePreviousCheckpoint(t *testing.T) {
	skipIfTestDataDirMissing(t)

	cptr, err := NewCursorAtPrevCheckpoint(testDataDir)
	if err != nil {
		t.Fatalf("error creating cursor: %v", err)
	}
	c := *cptr

	expectedCounts := map[RecordType]uint64{
		Insert: 200,
		Update: 200,
		Delete: 200,
	}
	actualCounts := make(map[RecordType]uint64)

	var ents []Entry

	for {
		ents, c, err = c.ReadEntries()
		if err == nil && ents != nil {
			for _, ent := range ents {
				actualCounts[ent.Type]++
			}
		} else {
			break
		}
	}

	for typ, expected := range expectedCounts {
		actual, ok := actualCounts[typ]
		if !ok || actual != expected {
			t.Errorf("incorrect count for type: %v; expected %v but got %v", typ, expected, actual)
		}
	}
}

func skipIfTestDataDirMissing(t *testing.T) {
	if _, err := os.Stat(testDataDir); err != nil && os.IsNotExist(err) {
		t.Skipf("you must make the test data directory with %q", prepareTestDataDir)
	}
}
