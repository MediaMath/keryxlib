package streams

import (
	"github.com/MediaMath/keryxlib/filters"
	"github.com/MediaMath/keryxlib/message"
	"github.com/MediaMath/keryxlib/pg"
)

type FullStream struct {
	walStream *WalStream
	sr        *pg.SchemaReader
}

func NewKeryxStream(sr *pg.SchemaReader) *FullStream {
	return &FullStream{nil, sr}
}

func (fs *FullStream) Stop() {
	if fs.walStream != nil {
		fs.walStream.Stop()
	}
}

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
