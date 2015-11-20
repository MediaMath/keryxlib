package streams

import (
	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg"
)

//FullStream is a facade around the full process of taking WAL entries and publishing them as txn messages.
type FullStream struct {
	walStream *WalStream
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
func (fs *FullStream) StartKeryxStream(filters filters.MessageFilter, dataDir string, bufferWorkingDirectory string) (<-chan *message.Transaction, error) {
	walStream, err := NewWalStream(dataDir)
	if err != nil {
		return nil, err
	}
	fs.walStream = walStream

	wal, err := fs.walStream.Start()
	if err != nil {
		return nil, err
	}

	txnBuffer := &TxnBuffer{filters, bufferWorkingDirectory}
	buffered, err := txnBuffer.Start(wal)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	populated := &PopulatedMessageStream{filters, fs.sr}
	keryx, err := populated.Start(buffered)
	if err != nil {
		fs.Stop()
		return nil, err
	}

	return keryx, nil
}
