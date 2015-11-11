package streamer

import (
	"fmt"
	"log"
	"time"

	"github.com/MediaMath/keryxlib/debug"
	"github.com/MediaMath/keryxlib/pg/wal"
)

// Streamer models the state of a streamer process
type Streamer struct {
	dataDir             string
	publish             chan<- *wal.Entry
	stop                chan interface{}
	cursor              *wal.Cursor
	lastOffsetPublished uint64
}

// New creates a new streamer with xlog directory
func New(dataDir string) (*Streamer, error) {
	s := &Streamer{dataDir, nil, make(chan interface{}), nil, 0}

	return s, nil
}

// Start begins streaming of events in a go routine and returns a channel of *xlog.XLogRecord
func (streamer *Streamer) Start(d debug.Outputter) (<-chan *wal.Entry, error) {
	out := make(chan *wal.Entry)

	if streamer.publish == nil {
		streamer.publish = out

		err := streamer.startAtCheckpoint()
		if err != nil {
			return nil, err
		}

		tick := time.Tick(50 * time.Millisecond)

		go func() {
			for !streamer.publishUntilErrorOrStopped(d) {
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
func (streamer *Streamer) Stop() {
	streamer.stop <- true
}

func (streamer *Streamer) isStopped() (stopped bool) {
	select {
	case <-streamer.stop:
		stopped = true
	default:
		stopped = false
	}

	return
}

func (streamer *Streamer) startAtCheckpoint() error {
	cursor, err := wal.NewCursorAtCheckpoint(streamer.dataDir)
	if err == nil {
		streamer.cursor = cursor
	}

	return err
}

func (streamer *Streamer) publishUntilErrorOrStopped(d debug.Outputter) (stopped bool) {
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
				d("read entry %v", ent)
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
