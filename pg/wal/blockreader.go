package wal

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"fmt"
	"os"
	"path/filepath"
)

type blockReader struct {
	dataDirPath string
	blockSize   uint32
	wordSize    uint32
}

func (b *blockReader) readBlock(location Location) []byte {
	filename := filepath.Join(b.dataDirPath, "pg_xlog", location.Filename())

	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	pageOffset := int64(location.StartOfPage().FromStartOfFile())

	block := make([]byte, b.blockSize)

	count, err := file.ReadAt(block, pageOffset)
	file.Close()

	if err != nil {
		panic(fmt.Errorf("failed to read block at 0x%.8X from %q: %v", pageOffset, filename, err))
	} else if int64(count) < int64(b.blockSize) {
		panic(fmt.Errorf("failed to read full block from %q: only read %v bytes", filename, count))
	}

	return block
}
