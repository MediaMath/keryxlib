package streams

import (
	"strings"
	"testing"

	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg/wal"
)

func TestCreateSummary(t *testing.T) {
	sr := &testSchemaReader{
		dbMap:  map[uint32]string{1: "foo"},
		relMap: map[uint32]string{1: "bar.boo", 99: "baz.moo"},
	}

	entries := make(chan []*wal.Entry, 10)

	entries <- []*wal.Entry{} //skipped

	entries <- []*wal.Entry{
		&wal.Entry{DatabaseID: 1, RelationID: 99, Type: wal.Insert},
		&wal.Entry{DatabaseID: 1, RelationID: 1, Type: wal.Insert},
		&wal.Entry{DatabaseID: 1, RelationID: 1, Type: wal.Insert},
		&wal.Entry{DatabaseID: 1, RelationID: 99, Type: wal.Update},
		&wal.Entry{DatabaseID: 1, RelationID: 1, Type: wal.Delete},
		&wal.Entry{Type: wal.Commit, TransactionID: 88, TimelineID: 99, ReadFrom: wal.NewLocation(1, 2, 3, 4, 5)},
	}

	entries <- []*wal.Entry{
		&wal.Entry{DatabaseID: 1, RelationID: 1, Type: wal.Delete},
		&wal.Entry{Type: wal.Commit, TransactionID: 93, TimelineID: 99, ReadFrom: wal.NewLocation(1, 2, 3, 4, 5)},
	}

	close(entries)

	summaries, err := SummaryStream{sr}.Start("boom", entries)

	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for summary := range summaries {
		count++
		if count > 2 {
			t.Fatal("Got more than 2 records")
		}

		switch count {
		case 1:
			if summary.ServerVersion != "boom" || summary.MessageCount != 6 || summary.TransactionID != 88 {
				t.Errorf("Incorrect summary for 1: %v", summary)
			}

			if len(summary.Tables) != 2 {
				t.Fatalf("Didn't get tables: %v", summary.Tables)
			}

			boo := summary.Tables[message.Table{DatabaseName: "foo", Namespace: "bar", Relation: "boo"}]
			moo := summary.Tables[message.Table{DatabaseName: "foo", Namespace: "baz", Relation: "moo"}]

			if boo.Inserts != 2 || boo.Deletes != 1 || boo.Updates != 0 {
				t.Errorf("Incorrect boo: %v", boo)
			}

			if moo.Inserts != 1 || moo.Deletes != 0 || moo.Updates != 1 {
				t.Errorf("Incorrect moo: %v", moo)
			}
		case 2:
			if summary.ServerVersion != "boom" || summary.MessageCount != 2 || summary.TransactionID != 93 {
				t.Errorf("Incorrect summary for 2: %v", summary)
			}

			if len(summary.Tables) != 1 {
				t.Fatalf("Didn't get tables: %v", summary.Tables)
			}

			boo := summary.Tables[message.Table{DatabaseName: "foo", Namespace: "bar", Relation: "boo"}]

			if boo.Inserts != 0 || boo.Deletes != 1 || boo.Updates != 0 {
				t.Errorf("Incorrect boo: %v", boo)
			}
		}
	}

	if count < 2 {
		t.Fatalf("Didn't get all summaries: %v", count)
	}
}

type testSchemaReader struct {
	dbMap  map[uint32]string
	relMap map[uint32]string
}

func (t *testSchemaReader) GetDatabaseName(databaseID uint32) string {
	return t.dbMap[databaseID]
}

func (t *testSchemaReader) GetNamespaceAndTable(databaseID uint32, relationID uint32) (string, string) {
	s := t.relMap[relationID]

	pairs := strings.Split(s, ".")
	if len(pairs) != 2 {
		return "", ""
	}

	return pairs[0], pairs[1]
}
