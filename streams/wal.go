package streams

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/MediaMath/keryxlib/pg/wal"
)

//WalStream is an abstraction around WAL entries.
type WalStream struct {
	dataDir             string
	publish             chan<- *wal.Entry
	stop                chan interface{}
	cursor              *wal.Cursor
	lastOffsetPublished uint64
}

// NewWalStream creates a new WalStream pointed at the provided dataDir
func NewWalStream(dataDir string) (*WalStream, error) {
	s := &WalStream{dataDir, nil, make(chan interface{}), nil, 0}

	return s, nil
}

// Start begins streaming of events in a go routine and returns a channel of WAL entries
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

	var ents []wal.Entry
	var previousCursor, currentCursor wal.Cursor
	var err error

	currentCursor = *streamer.cursor
	keepReading := true
	for keepReading {

		previousCursor = currentCursor

		ents, currentCursor, err = currentCursor.ReadEntries()

		if err == nil && len(ents) > 0 {
			for _, ent := range ents {
				if ent.ReadFrom.Offset() > streamer.lastOffsetPublished {
					streamer.publish <- &ent
					*streamer.cursor = currentCursor

					streamer.lastOffsetPublished = ent.ReadFrom.Offset()
				}
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
		//the file can not exist for 2 reasons
		//1 - can happen a lot, if keryx is staying ahead of the wal log
		//2 - hopefully not often, if keryx is falling too far behind and the wal is being removed
		//we are gambling on number 1 being the cause of this error and therefore are not logging it as it is very
		//noisy if we do
		if !os.IsNotExist(err) {
			log.Printf("error while reading wal: %v", err)
		}
		streamer.startAtCheckpoint()
	}

	return
}
