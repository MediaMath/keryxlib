package pg

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func writePgVersionFile(t *testing.T, version string) string {
	tmpDir, err := ioutil.TempDir("", "keryxlib-TestPGVersion")
	if err != nil {
		t.Fatal(err)
	}

	err = ioutil.WriteFile(path.Join(tmpDir, "PG_VERSION"), []byte(version), 0664)
	if err != nil {
		t.Fatal(err)
	}

	return tmpDir
}

func TestPGVersion(t *testing.T) {

	tmpDir := writePgVersionFile(t, "9.4")
	defer os.RemoveAll(tmpDir)

	versionNumber, err := DetectPgVersion(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	if versionNumber != "9.4" {
		t.Errorf("PG_VERSION should be 9.4 was %v", versionNumber)
	}
}

func TestIsSupported(t *testing.T) {

	tmpDir1 := writePgVersionFile(t, "9.4")
	defer os.RemoveAll(tmpDir1)

	err := IsPgVersionSupported(tmpDir1)
	if err == nil {
		t.Error("Did not fail on unsupported version")
	}

	tmpDir2 := writePgVersionFile(t, "9.1")
	defer os.RemoveAll(tmpDir2)

	err = IsPgVersionSupported(tmpDir2)
	if err != nil {
		t.Error(err)
	}
}
