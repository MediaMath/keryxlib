package streams

import (
	"fmt"
	"log"
	"time"

	"github.com/MediaMath/keryxlib/pg/wal"
)

// Streamer models the state of a streamer process
type WalStream struct {
	dataDir             string
	publish             chan<- *wal.Entry
	stop                chan interface{}
	cursor              *wal.Cursor
	lastOffsetPublished uint64
}

// New creates a new streamer with xlog directory
func NewWalStream(dataDir string) (*WalStream, error) {
	s := &WalStream{dataDir, nil, make(chan interface{}), nil, 0}

	return s, nil
}

// Start begins streaming of events in a go routine and returns a channel of *xlog.XLogRecord
func (streamer *WalStream) Start() (<-chan *wal.Entry, error) {
	out := make(chan *wal.Entry)

	if streamer.publish == nil {
		streamer.publish = out

		err := streamer.startAtCheckpoint()
		if err != nil {
			return nil, err
		}

		tick := time.Tick(50 * time.Millisecond)

		go func() {
			for !streamer.publishUntilErrorOrStopped() {
				<-tick
			}
			close(out)
			streamer.publish = nil
		}()
	} else {
		return nil, fmt.Errorf("already publishing")
	}

	return out, nil
}

// Stop will end publishing to the channel returned by start.
func (streamer *WalStream) Stop() {
	streamer.stop <- true
}

func (streamer *WalStream) isStopped() (stopped bool) {
	select {
	case <-streamer.stop:
		stopped = true
	default:
		stopped = false
	}

	return
}

func (streamer *WalStream) startAtCheckpoint() error {
	cursor, err := wal.NewCursorAtCheckpoint(streamer.dataDir)
	if err == nil {
		streamer.cursor = cursor
	}

	return err
}

func (streamer *WalStream) publishUntilErrorOrStopped() (stopped bool) {
	stopped = false

	var ent *wal.Entry
	var previousCursor, currentCursor wal.Cursor
	var err error

	currentCursor = *streamer.cursor
	keepReading := true
	for keepReading {

		previousCursor = currentCursor

		ent, currentCursor, err = currentCursor.ReadEntry()

		if err == nil && ent != nil {
			if ent.ReadFrom.Offset() > streamer.lastOffsetPublished {
				streamer.publish <- ent
				*streamer.cursor = currentCursor

				streamer.lastOffsetPublished = ent.ReadFrom.Offset()
			}
		} else {
			keepReading = false
		}

		stopped = streamer.isStopped()
		keepReading = keepReading && !stopped

		if previousCursor.String() == currentCursor.String() {
			keepReading = false
		}
	}

	if err != nil {
		log.Printf("error while reading wal: %v", err)
		streamer.startAtCheckpoint()
	}

	return
}
