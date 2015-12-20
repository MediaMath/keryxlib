package tko

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/codegangsta/cli"
)

//GetConditionFromArgsOrStdIn will attempt to parse the first argument as a tko condition.
//If no args are available it will instead try to parse stdin
func GetConditionFromArgsOrStdIn(ctx *cli.Context) (Condition, error) {
	if len(ctx.Args()) > 0 {
		return ReadConditionFromJSON(ctx.Args()[0])
	}

	if doesFileHaveInput(os.Stdin) {
		bytes, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			return nil, fmt.Errorf("Error reading stdin: %v", err)
		}

		return ReadConditionFromJSON(string(bytes))
	}

	return nil, fmt.Errorf("No query provided.")
}

func doesFileHaveInput(file *os.File) bool {
	if file == nil {
		return false
	}

	stat, _ := file.Stat()
	return stat.Mode()&os.ModeCharDevice == 0
}
