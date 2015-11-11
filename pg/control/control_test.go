package control

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
)

//3 files are saved with repo for testing purposes
const PGControlTestDir string = "./test_data/global/"

var dataFiles = []string{"pg_control", "pg_control2", "pg_control3"}

func FailIfError(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func FailIfTrue(t *testing.T, val bool, msg string) {
	if val {
		t.Fatal(msg)
	}
}

type FileType uint32

const (
	FILE FileType = iota
	DIRECTORY
)

func GetDirectoryListing(dir string, fileType FileType, urls ...bool) ([]string, error) {

	fileUrls := false

	if urls != nil && len(urls) > 0 {
		fileUrls = urls[0]
	}

	listing, err := ioutil.ReadDir(dir)

	if err != nil {
		return nil, err
	}

	var fileNames []string = make([]string, 0)

	// var i int = 0
	for _, value := range listing {
		if (fileType == FILE && value.IsDir()) || fileType == DIRECTORY && !value.IsDir() {
			continue
		}

		if fileUrls {
			fileNames = append(fileNames, fmt.Sprintf("file://%s", filepath.Join(dir, value.Name())))
		} else {
			fileNames = append(fileNames, value.Name())
		}
	}

	return fileNames, nil
}

func TestPgDataFileExists(t *testing.T) {
	filesInDir, err := GetDirectoryListing(PGControlTestDir, 0)
	FailIfError(t, err)

	FailIfTrue(t, filesInDir[0] != dataFiles[0], filesInDir[0]+"is not expected file")
	FailIfTrue(t, filesInDir[1] != dataFiles[1], filesInDir[1]+"is not expected file")
	FailIfTrue(t, filesInDir[2] != dataFiles[2], filesInDir[2]+"is not expected file")
}

func TestPgControlSystemIdentifier(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.SystemIdentifier != 6036814056215492206, "System identifier can't be null or zero")
	FailIfTrue(t, pgData2.SystemIdentifier != 6127242208946681587, "System identifier can't be null or zero")
	FailIfTrue(t, pgData3.SystemIdentifier != 6093161261192693735, "System identifier can't be null or zero")
}

func TestPgControlVersion(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.Version != 903, "pg control version needs to be 903")
	FailIfTrue(t, pgData2.Version != 903, "pg control version needs to be 903")
	FailIfTrue(t, pgData3.Version != 903, "pg control version needs to be 903")
}

func TestPgCatalogVersion(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.CatalogVersionNo != 201105231, "Catalog version is not 201105231")
	FailIfTrue(t, pgData2.CatalogVersionNo != 201105231, "Catalog version is not 201105231")
	FailIfTrue(t, pgData3.CatalogVersionNo != 201105231, "Catalog version is not 201105231")

}

func TestDBState(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.State <= 0 && pgData1.State > 6, pgData1.State.String()+" is not a valid DB State")
	FailIfTrue(t, pgData2.State <= 0 && pgData2.State > 6, pgData2.State.String()+" is not a valid DB State")
	FailIfTrue(t, pgData3.State <= 0 && pgData3.State > 6, pgData3.State.String()+" is not a valid DB State")
}

func TestDBTime(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.Time != 1432066899, "time should be 1432066899 and a timestamp")
	FailIfTrue(t, pgData2.Time != 1427303514, "time should be 1427303514 and a timestamp")
	FailIfTrue(t, pgData3.Time != 1427825008, "time should be 1427825008 and a timestamp")
}

func TestDBBlockSize(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.Blcksz != 8192, "DB Block size must be 8192")
	FailIfTrue(t, pgData2.Blcksz != 8192, "DB Block size must be 8192")
	FailIfTrue(t, pgData3.Blcksz != 8192, "DB Block size must be 8192")
}

func TestWALBlockSize(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.XlogBlcksz != 8192, "WAL Block size must be 8192")
	FailIfTrue(t, pgData2.XlogBlcksz != 8192, "WAL Block size must be 8192")
	FailIfTrue(t, pgData3.XlogBlcksz != 8192, "WAL Block size must be 8192")
}

func TestToastChunckSize(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile(t)

	FailIfTrue(t, pgData1.ToastMaxChunkSize != 1996, "Toast chunk maximum size must be 1996")
	FailIfTrue(t, pgData2.ToastMaxChunkSize != 1996, "Toast chunk maximum size must be 1996")
	FailIfTrue(t, pgData3.ToastMaxChunkSize != 1996, "Toast chunk maximum size must be 1996")
}

// helper function to return pgData for the files
func readPGDataFile(t *testing.T) (*Control, *Control, *Control) {
	pgData1, err1 := NewControlFromFile(PGControlTestDir + dataFiles[0])
	FailIfError(t, err1)

	pgData2, err2 := NewControlFromFile(PGControlTestDir + dataFiles[1])
	FailIfError(t, err2)

	pgData3, err3 := NewControlFromFile(PGControlTestDir + dataFiles[2])
	FailIfError(t, err3)

	return pgData1, pgData2, pgData3
}
