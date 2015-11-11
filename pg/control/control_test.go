package control

import (
	"testing"

	"github.com/MediaMath/Keryx/testingUtils"
	"github.com/MediaMath/Keryx/utils"
)

//3 files are saved with repo for testing purposes
const PGControlTestDir string = "./test_data/global/"

var dataFiles = []string{"pg_control", "pg_control2", "pg_control3"}

func TestPgDataFileExists(t *testing.T) {
	filesInDir, err := utils.GetDirectoryListing(PGControlTestDir, 0)
	testingUtils.FailIfError(t, err)

	testingUtils.FailIfTrue(t, filesInDir[0] != dataFiles[0], filesInDir[0]+"is not expected file")
	testingUtils.FailIfTrue(t, filesInDir[1] != dataFiles[1], filesInDir[1]+"is not expected file")
	testingUtils.FailIfTrue(t, filesInDir[2] != dataFiles[2], filesInDir[2]+"is not expected file")
}

func TestPgControlSystemIdentifier(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.SystemIdentifier != 6036814056215492206, "System identifier can't be null or zero")
	testingUtils.FailIfTrue(t, pgData2.SystemIdentifier != 6127242208946681587, "System identifier can't be null or zero")
	testingUtils.FailIfTrue(t, pgData3.SystemIdentifier != 6093161261192693735, "System identifier can't be null or zero")
}

func TestPgControlVersion(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.Version != 903, "pg control version needs to be 903")
	testingUtils.FailIfTrue(t, pgData2.Version != 903, "pg control version needs to be 903")
	testingUtils.FailIfTrue(t, pgData3.Version != 903, "pg control version needs to be 903")
}

func TestPgCatalogVersion(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.CatalogVersionNo != 201105231, "Catalog version is not 201105231")
	testingUtils.FailIfTrue(t, pgData2.CatalogVersionNo != 201105231, "Catalog version is not 201105231")
	testingUtils.FailIfTrue(t, pgData3.CatalogVersionNo != 201105231, "Catalog version is not 201105231")

}

func TestDBState(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.State <= 0 && pgData1.State > 6, pgData1.State.String()+" is not a valid DB State")
	testingUtils.FailIfTrue(t, pgData2.State <= 0 && pgData2.State > 6, pgData2.State.String()+" is not a valid DB State")
	testingUtils.FailIfTrue(t, pgData3.State <= 0 && pgData3.State > 6, pgData3.State.String()+" is not a valid DB State")
}

func TestDBTime(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.Time != 1432066899, "time should be 1432066899 and a timestamp")
	testingUtils.FailIfTrue(t, pgData2.Time != 1427303514, "time should be 1427303514 and a timestamp")
	testingUtils.FailIfTrue(t, pgData3.Time != 1427825008, "time should be 1427825008 and a timestamp")
}

func TestDBBlockSize(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.Blcksz != 8192, "DB Block size must be 8192")
	testingUtils.FailIfTrue(t, pgData2.Blcksz != 8192, "DB Block size must be 8192")
	testingUtils.FailIfTrue(t, pgData3.Blcksz != 8192, "DB Block size must be 8192")
}

func TestWALBlockSize(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.XlogBlcksz != 8192, "WAL Block size must be 8192")
	testingUtils.FailIfTrue(t, pgData2.XlogBlcksz != 8192, "WAL Block size must be 8192")
	testingUtils.FailIfTrue(t, pgData3.XlogBlcksz != 8192, "WAL Block size must be 8192")
}

func TestToastChunckSize(t *testing.T) {

	pgData1, pgData2, pgData3 := readPGDataFile()

	testingUtils.FailIfTrue(t, pgData1.ToastMaxChunkSize != 1996, "Toast chunk maximum size must be 1996")
	testingUtils.FailIfTrue(t, pgData2.ToastMaxChunkSize != 1996, "Toast chunk maximum size must be 1996")
	testingUtils.FailIfTrue(t, pgData3.ToastMaxChunkSize != 1996, "Toast chunk maximum size must be 1996")
}

// helper function to return pgData for the files
func readPGDataFile() (*Control, *Control, *Control) {
	pgData1, err1 := NewControlFromFile(PGControlTestDir + dataFiles[0])
	utils.LogFatalOnError(err1)

	pgData2, err2 := NewControlFromFile(PGControlTestDir + dataFiles[1])
	utils.LogFatalOnError(err2)

	pgData3, err3 := NewControlFromFile(PGControlTestDir + dataFiles[2])
	utils.LogFatalOnError(err3)

	return pgData1, pgData2, pgData3
}
