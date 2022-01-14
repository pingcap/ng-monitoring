package printer

import (
	"fmt"
	_ "runtime" // import link package
	_ "unsafe"  // required by go:linkname

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Version information.
var (
	NGMBuildTS   = "None"
	NGMGitHash   = "None"
	NGMGitBranch = "None"
)

//go:linkname buildVersion runtime.buildVersion
var buildVersion string

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
