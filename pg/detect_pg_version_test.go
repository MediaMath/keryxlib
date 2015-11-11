package pg

import (
	"testing"

	"github.com/MediaMath/Keryx/testingUtils"
	"github.com/MediaMath/Keryx/utils"
)

func TestPGVersion(t *testing.T) {

	f, _, _, path, err := utils.GenerateRandomFileForUse("9.4")
	testingUtils.FailIfError(t, err)
	defer f()

	versionNumber := DetectPgVersion(path)

	if versionNumber != "9.1" {
		testingUtils.FailIfError(t, err, "PG_VERSION should be 9.1")
	}
}
