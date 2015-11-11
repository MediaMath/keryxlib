package debug

import (
	"fmt"
	"log"
	"os"
)

type Outputter func(string, ...interface{})

func NullOutputter(string, ...interface{}) {}

func LogOutputter(msg string, v ...interface{}) {
	log.Printf(msg, v...)
}

func FileOutputter(debugFileName string) (Outputter, error) {
	if debugFileName == "" {
		return NullOutputter, nil
	}

	debugLogFile, err := os.OpenFile(debugFileName, os.O_SYNC|os.O_WRONLY|os.O_CREATE, 0666)

	if err != nil {
		return nil, err
	}

	return func(msg string, v ...interface{}) { debugLogFile.Write([]byte(fmt.Sprintf(msg, v...) + "\n")) }, nil
}

func SuppressDuplicateOutputter(level int, count int, out Outputter) Outputter {
	vals := duplicateFilteringOutputter{level, count, ""}
	return func(msg string, v ...interface{}) {
		output := fmt.Sprintf(msg, v)

		if output != vals.message {
			if vals.count > 0 {
				out("%s X %d", vals.message, vals.count)
				vals.count = 0
			}

			vals.message = output

		} else {
			vals.count++
		}

		if vals.count <= vals.level {
			out("%s", output)
		}
	}
}

type duplicateFilteringOutputter struct {
	level   int
	count   int
	message string
}
