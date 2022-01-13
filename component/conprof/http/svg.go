package http

import (
	"bytes"
	"flag"
	"io"
	"strconv"
	"time"

	graphviz "github.com/goccy/go-graphviz"
	"github.com/google/pprof/driver"
	"github.com/google/pprof/profile"
)

func ConvertToSVG(protoData []byte) ([]byte, error) {
	p, err := profile.ParseData(protoData)
	if err != nil {
		return nil, err
	}

	dotData, err := convertToDot(p)
	if err != nil {
		return nil, err
	}

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

func convertToDot(p *profile.Profile) ([]byte, error) {
	args := []string{
		"-dot",
		// prevent printing stdout
		"-output", "dummy",
		"-seconds", strconv.Itoa(int(30)),
	}
	// mock address
	args = append(args, "127.0.0.1:10080")
	f := &flagSet{
		FlagSet: flag.NewFlagSet("pprof", flag.PanicOnError),
		args:    args,
	}

	bufw := &bufWriteCloser{Buffer: bytes.NewBuffer(nil)}
	err := driver.PProf(&driver.Options{
		Fetch:   &localProfileFetcher{p: p},
		Flagset: f,
		UI:      &blankPprofUI{},
		Writer:  bufw,
	})
	return bufw.Bytes(), err
}

type bufWriteCloser struct {
	*bytes.Buffer
}

func (o *bufWriteCloser) Open(_ string) (io.WriteCloser, error) {
	return o, nil
}

func (o *bufWriteCloser) Close() error {
	return nil
}

type localProfileFetcher struct {
	p *profile.Profile
}

func (s *localProfileFetcher) Fetch(src string, duration, timeout time.Duration) (*profile.Profile, string, error) {
	return s.p, "", nil
}

type flagSet struct {
	*flag.FlagSet
	args []string
}

func (f *flagSet) StringList(o, d, c string) *[]*string {
	return &[]*string{f.String(o, d, c)}
}

func (f *flagSet) ExtraUsage() string {
	return ""
}

func (f *flagSet) Parse(usage func()) []string {
	f.Usage = usage
	_ = f.FlagSet.Parse(f.args)
	return f.Args()
}

func (f *flagSet) AddExtraUsage(eu string) {}

// blankPprofUI is used to eliminate the pprof logs
type blankPprofUI struct {
}

func (b blankPprofUI) ReadLine(prompt string) (string, error) {
	panic("not support")
}

func (b blankPprofUI) Print(i ...interface{}) {
}

func (b blankPprofUI) PrintErr(i ...interface{}) {
}

func (b blankPprofUI) IsTerminal() bool {
	return false
}

func (b blankPprofUI) WantBrowser() bool {
	return false
}

func (b blankPprofUI) SetAutoComplete(complete func(string) string) {
}
