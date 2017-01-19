package pg

// Copyright 2015 MediaMath <http://www.mediamath.com>.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

import (
	"errors"
	"io/ioutil"
	"path"
	"strings"
)

//ErrIncorrectVersion is returned when a non supported postgres is found.
var ErrIncorrectVersion = errors.New("only postgres 9.1 is supported")

//IsPgVersionSupported returns an error if the postgres version is not supported currently.
func IsPgVersionSupported(versionFilePath string) error {
	versionNumber, err := DetectPgVersion(versionFilePath)

	if err == nil && versionNumber != "9.1" {
		err = ErrIncorrectVersion
	}

	return err
}

//DetectPgVersion attempts to determine what version of postgres a data directory is based on.
func DetectPgVersion(versionFilePath string) (versionNumber string, err error) {

	versionFileName := path.Join(versionFilePath, "PG_VERSION")
	versionFile, err := ioutil.ReadFile(versionFileName)

	if err == nil {
		versionNumber = strings.TrimSpace(string(versionFile))
	}

	return
}
