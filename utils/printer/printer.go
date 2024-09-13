package printer

import (
	"fmt"
	"runtime"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	_ "unsafe" // required by go:linkname
)

// Version information.
var (
	NGMBuildTS   = "None"
	NGMGitHash   = "None"
	NGMGitBranch = "None"
)

var buildVersion string

func init() {
	buildVersion = runtime.Version()
}

// PrintNGMInfo prints the NGM version information.
func PrintNGMInfo() {
	log.Info("Welcome to ng-monitoring.",
		zap.String("Git Commit Hash", NGMGitHash),
		zap.String("Git Branch", NGMGitBranch),
		zap.String("UTC Build Time", NGMBuildTS),
		zap.String("GoVersion", buildVersion))
}

func GetNGMInfo() string {
	return fmt.Sprintf("Git Commit Hash: %s\n"+
		"Git Branch: %s\n"+
		"UTC Build Time: %s\n"+
		"GoVersion: %s",
		NGMGitHash,
		NGMGitBranch,
		NGMBuildTS,
		buildVersion)
}
