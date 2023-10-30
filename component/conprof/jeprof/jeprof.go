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
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(stdout)
	if err != nil {
		return nil, err
	}
	errMsg, err := io.ReadAll(stderr)
	if err != nil {
		return nil, err
	}
	err = cmd.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch tikv heap profile: %s", errMsg)
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
