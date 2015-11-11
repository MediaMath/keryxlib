package pg

import (
	"github.com/MediaMath/Keryx/utils"
	"io/ioutil"
	"path"
	"strings"
)

func IsPgVersionSupported(versionFilePath string) error {
	versionNumber := DetectPgVersion(versionFilePath)

	var err error
	var message string = "Only postgres 9.1 is supported."
	if versionNumber != "9.1" {
		err = utils.GenerateError(message)
	}
	return err
}

func DetectPgVersion(versionFilePath string) string {

	versionFileName := ""

	if strings.Contains(versionFilePath, "random_file") {
		versionFileName = versionFilePath

		fileExists := utils.FileExistsAndIsNotDirectory(versionFileName)

		if !fileExists {
			versionFileName = path.Join(versionFilePath, "PG_VERSION")
		}

	} else {
		versionFileName = path.Join(versionFilePath, "PG_VERSION")
	}

	versionFile, err := ioutil.ReadFile(versionFileName)
	utils.LogFatalOnError(err)

	versionNumber := strings.TrimSpace(string(versionFile))

	return versionNumber
}
