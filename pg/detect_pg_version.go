package pg

import (
	"errors"
	"io/ioutil"
	"path"
	"strings"
)

var incorrectVersionErr error = errors.New("Only postgres 9.1 is supported.")

func IsPgVersionSupported(versionFilePath string) error {
	versionNumber, err := DetectPgVersion(versionFilePath)

	if err == nil && versionNumber != "9.1" {
		err = incorrectVersionErr
	}

	return err
}

func DetectPgVersion(versionFilePath string) (versionNumber string, err error) {

	versionFileName := path.Join(versionFilePath, "PG_VERSION")
	versionFile, err := ioutil.ReadFile(versionFileName)

	if err == nil {
		versionNumber = strings.TrimSpace(string(versionFile))
	}

	return
}
