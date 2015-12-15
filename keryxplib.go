package keryxplib

import (
	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg"
	"github.com/MediaMath/keryxlib/streams"
)

//FullStream is a facade around the full process of taking WAL entries and publishing them as txn messages.
type FullStream struct {
	walStream *streams.WalStream
	sr        *pg.SchemaReader
}

//NewKeryxStream takes a schema reader and returns a FullStream
func NewKeryxStream(sr *pg.SchemaReader) *FullStream {
	return &FullStream{nil, sr}
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

	txnBuffer := &streams.TxnBuffer{Filters: filters, WorkingDirectory: bufferWorkingDirectory}
	buffered, err := txnBuffer.Start(wal)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	populated := &streams.PopulatedMessageStream{Filter: filters, SchemaReader: fs.sr}
	keryx, err := populated.Start(serverVersion, buffered)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	return keryx, nil
}
