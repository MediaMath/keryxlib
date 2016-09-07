package streams

import (
	"time"

	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg/wal"
)

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//SummaryStream returns a stream of message.TxnSummary
type SummaryStream struct {
	SchemaMetaInformation
}

//SchemaMetaInformation provides textual information from wal log entry ids
type SchemaMetaInformation interface {
	GetDatabaseName(databaseID uint32) string
	GetNamespaceAndTable(databaseID uint32, relationID uint32) (string, string)
}

//Start takes the buffered wal log entries and creates message summaries for them
func (s SummaryStream) Start(serverVersion string, entryChan <-chan []*wal.Entry) (<-chan message.TxnSummary, error) {
	txns := make(chan message.TxnSummary)

	go func() {
		for entries := range entryChan {
			if len(entries) > 0 {
				txn := message.TxnSummary{}
				txn.ServerVersion = serverVersion

				commit := entries[len(entries)-1]
				txn.TransactionID = commit.TransactionID
				txn.CommitKey = createKey(commit)
				txn.CommitTime = time.Unix(0, commit.ParseTime).UTC()
				txn.MessageCount = len(entries)

				txn.Tables = make(map[message.Table]message.Summary)
				for _, entry := range entries {
					if entry.Type == wal.Insert || entry.Type == wal.Update || entry.Type == wal.Delete {
						table := message.Table{}
						table.DatabaseName = s.GetDatabaseName(entry.DatabaseID)
						table.Namespace, table.Relation = s.GetNamespaceAndTable(entry.DatabaseID, entry.RelationID)

						summary := txn.Tables[table]

						switch entry.Type {
						case wal.Insert:
							summary.Inserts++
						case wal.Update:
							summary.Updates++
						case wal.Delete:
							summary.Deletes++
						}

						txn.Tables[table] = summary
					}
				}

				txn.PublishTime = time.Now().UTC()
				txns <- txn
			}
		}
		close(txns)
	}()

	return txns, nil
}
