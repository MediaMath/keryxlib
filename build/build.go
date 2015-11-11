package build

import "fmt"

var buildNumber string
var buildTime string
var buildSHA string

// BuildString generates a string describing the running build
func BuildString() string {
	if buildNumber == "" {
		return "No Build Information"
	}

	return fmt.Sprintf("Build: %v Build Time: %v SHA: %v", buildNumber, buildTime, buildSHA)
}

// BuildNumber returns the number of the running build
func BuildNumber() string {
	return buildNumber
}
