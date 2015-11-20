package message

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	bufferFileSuffix = "buffer_data"
)

//Buffer is a collection of included data by transaction.
type Buffer struct {
	workingDirectory string
	memoryBuffer     map[uint32][]byte
	memoryLimit      uint64
	itemSize         uint64
	memoryCounter    uint64
}

//NewBuffer returns a new abstraction of data by transaction.
func NewBuffer(workingDirectory string, memoryLimit, itemSize uint64) *Buffer {
	b := Buffer{workingDirectory, nil, memoryLimit, itemSize, 0}
	b.initialize()
	return &b
}

func (b *Buffer) initialize() {
	// wipe directory of buffer files
	files, err := ioutil.ReadDir(b.workingDirectory)
	if err != nil {
		log.Printf("error initializing buffer in directory %v: %v", b.workingDirectory, err)
	} else {
		for _, f := range files {
			if strings.HasSuffix(f.Name(), bufferFileSuffix) {
				os.Remove(filepath.Join(b.workingDirectory, f.Name()))
			}
		}
	}

	b.memoryBuffer = make(map[uint32][]byte)
	b.memoryCounter = 0
}

//Add adds data to the buffer by transaction id
func (b *Buffer) Add(key uint32, item []byte) {
	if (b.memoryCounter + b.itemSize) <= b.memoryLimit {
		b.addInMemory(key, item)
	} else {
		b.addOnDisk(key, item)
	}
}

//Remove removes data for a given transaction id and returns it.
func (b *Buffer) Remove(key uint32) (out [][]byte) {
	out, ok := b.removeFromMemory(key)
	if !ok {
		out = b.removeFromDisk(key)
	}
	return
}

func (b *Buffer) addInMemory(key uint32, src []byte) {
	dst := make([]byte, b.itemSize)
	copy(dst, src)
	b.memoryBuffer[key] = append(b.memoryBuffer[key], dst...)
	b.memoryCounter += b.itemSize
}

func (b *Buffer) removeFromMemory(key uint32) (out [][]byte, ok bool) {
	itemBuffer, ok := b.memoryBuffer[key]
	if ok {
		delete(b.memoryBuffer, key)
		out = extractItems(itemBuffer, b.itemSize)
	}

	return
}

func (b *Buffer) addOnDisk(key uint32, item []byte) {
	filename := filenameForKey(key, b.workingDirectory)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0660)
	if err != nil {
		log.Printf("error opening disk buffer file %v: %v", filename, err)
	} else {
		defer file.Close()

		memoryItems, ok := b.removeFromMemory(key)
		if ok {
			for _, memoryItem := range memoryItems {
				writeToFile(memoryItem, file)
			}
		}

		writeToFile(item, file)
	}
}

func writeToFile(bs []byte, file *os.File) {
	n, err := file.Write(bs)
	if err != nil {
		log.Printf("error writing to disk buffer file %v: %v", file.Name(), err)
	} else if len(bs) != n {
		log.Printf("error writing to disk buffer file %v: expected to write %v bytes but wrote %v instead", file.Name(), len(bs), n)
	}
}

func (b *Buffer) removeFromDisk(key uint32) (out [][]byte) {
	filename := filenameForKey(key, b.workingDirectory)
	itemBuffer, err := ioutil.ReadFile(filename)
	if err == nil {
		out = extractItems(itemBuffer, b.itemSize)
	}
	os.Remove(filename)
	return
}

func extractItems(itemBuffer []byte, itemSize uint64) (out [][]byte) {
	iblen := uint64(len(itemBuffer))
	for start, end := uint64(0), itemSize; end <= iblen; start, end = end, end+itemSize {
		out = append(out, itemBuffer[start:end])
	}
	return
}

func filenameForKey(key uint32, workingDirectory string) string {
	return filepath.Join(workingDirectory, fmt.Sprintf("%v.%v", key, bufferFileSuffix))
}
