package jeprof

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	graphviz "github.com/goccy/go-graphviz"
	"github.com/prometheus/common/config"
)

//go:embed jeprof.in
var jeprof string

func FetchRaw(url string, cfg config.HTTPClientConfig) ([]byte, error) {
	cmd := exec.Command("perl", "/dev/stdin", "--raw", url) //nolint:gosec
	cmd.Stdin = strings.NewReader(jeprof)
	cmd.Env = prepareJeprofEnv()
	if len(cfg.TLSConfig.CertFile) != 0 && len(cfg.TLSConfig.KeyFile) != 0 {
		cmd.Env = append(cmd.Env, fmt.Sprintf(
			"URL_FETCHER=curl -s --cert %s --key %s --cacert %s",
			cfg.TLSConfig.CertFile,
			cfg.TLSConfig.KeyFile,
			cfg.TLSConfig.CAFile,
		))
	}
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

	cmd := exec.Command("perl", "/dev/stdin", "--dot", f.Name()) //nolint:gosec
	cmd.Stdin = strings.NewReader(jeprof)
	cmd.Env = prepareJeprofEnv()
	dotContent, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	svgContent, err := convertDotToSVG(dotContent)
	if err != nil {
		return nil, err
	}
	return svgContent, nil
}

func convertDotToSVG(dotData []byte) ([]byte, error) {
	g := graphviz.New()
	graph, err := graphviz.ParseBytes(dotData)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewBuffer(nil)
	err = g.Render(graph, graphviz.SVG, buf)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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
	cmd.Env = prepareJeprofEnv()
	textContent, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return textContent, nil
}

func prepareJeprofEnv() []string {
	env := os.Environ()
	for _, key := range []string{"LANG", "LC_ALL", "LC_CTYPE"} {
		value, ok := getEnvValue(env, key)
		if !ok || value == "" || strings.EqualFold(value, "C.UTF-8") {
			env = setEnvValue(env, key, "C")
		}
	}
	return env
}

func getEnvValue(env []string, key string) (string, bool) {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix), true
		}
	}
	return "", false
}

func setEnvValue(env []string, key, value string) []string {
	prefix := key + "="
	for i, item := range env {
		if strings.HasPrefix(item, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}
