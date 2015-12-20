package keryxlib

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg"
	"github.com/MediaMath/keryxlib/streams"
)

//TransactionChannel sets up a keryx stream and schema reader with the provided configuration and returns
//it as a channel
func TransactionChannel(serverVersion string, kc *Config) (<-chan *message.Transaction, error) {

	schemaReader, err := pg.NewSchemaReader(kc.PGConnStrings, "postgres", 255)
	if err != nil {
		return nil, err
	}

	bufferWorkingDirectory, err := kc.GetBufferDirectoryOrTemp()
	if err != nil {
		return nil, err
	}

	f := filters.Exclusive(schemaReader, kc.ExcludeRelations)
	if len(kc.IncludeRelations) > 0 {
		f = filters.Inclusive(schemaReader, kc.IncludeRelations)
	}

	stream := NewKeryxStream(schemaReader, kc.MaxMessagePerTxn)
	return stream.StartKeryxStream(serverVersion, f, kc.DataDir, bufferWorkingDirectory)
}

//FullStream is a facade around the full process of taking WAL entries and publishing them as txn messages.
type FullStream struct {
	walStream       *streams.WalStream
	sr              *pg.SchemaReader
	MaxMessageCount uint
}

//NewKeryxStream takes a schema reader and returns a FullStream
func NewKeryxStream(sr *pg.SchemaReader, maxMessageCount uint) *FullStream {
	return &FullStream{nil, sr, maxMessageCount}
}

//Stop will end the reading on the WAL log and subsequent streams will therefore end.
func (fs *FullStream) Stop() {
	if fs.walStream != nil {
		fs.walStream.Stop()
	}
}

//StartKeryxStream will start all the streams necessary to go from WAL entries to txn messages.
func (fs *FullStream) StartKeryxStream(serverVersion string, filters filters.MessageFilter, dataDir string, bufferWorkingDirectory string) (<-chan *message.Transaction, error) {
	walStream, err := streams.NewWalStream(dataDir)
	if err != nil {
		return nil, err
	}
	fs.walStream = walStream

	wal, err := fs.walStream.Start()
	if err != nil {
		return nil, err
	}

	txnBuffer := &streams.TxnBuffer{Filters: filters, WorkingDirectory: bufferWorkingDirectory, SchemaReader: fs.sr}
	buffered, err := txnBuffer.Start(wal)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	populated := &streams.PopulatedMessageStream{Filters: filters, SchemaReader: fs.sr, MaxMessageCount: fs.MaxMessageCount}
	keryx, err := populated.Start(serverVersion, buffered)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	return keryx, nil
}
