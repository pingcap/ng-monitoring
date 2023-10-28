package jeprof

import (
	_ "embed"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

//go:embed jeprof.in
var jeprof string

func FetchRaw(url string) ([]byte, error) {
	cmd := exec.Command("perl", "/dev/stdin", "--raw", url) //nolint:gosec
	cmd.Stdin = strings.NewReader(jeprof)
	stderr, _ := cmd.StderrPipe()
	// use jeprof to fetch tikv heap profile
	data, err := cmd.Output()
	if err != nil {
		if stderr != nil {
			errMsg, _ := io.ReadAll(stderr)
			return nil, fmt.Errorf("failed to fetch tikv heap profile: %s", errMsg)
		} else {
			return nil, fmt.Errorf("failed to fetch tikv heap profile: %s", err)
		}
	}
	return data, nil
}

func ConvertToSVG(data []byte) ([]byte, error) {
	f, err := os.CreateTemp("", "prof")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command("perl", "/dev/stdin", "--svg", f.Name()) //nolint:gosec
	cmd.Stdin = strings.NewReader(jeprof)
	svgContent, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return svgContent, nil
}

func ConvertToText(data []byte) ([]byte, error) {
	f, err := os.CreateTemp("", "prof")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(data)
	if err != nil {
		return nil, err
	}

	// Brendan Gregg's collapsed stack format
	cmd := exec.Command("perl", "/dev/stdin", "--collapsed", f.Name()) //nolint:gosec
	cmd.Stdin = strings.NewReader(jeprof)
	textContent, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return textContent, nil
}
