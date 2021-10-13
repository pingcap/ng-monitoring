package config

import (
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	config := defaultConfig
	config.PD.Endpoints = append(config.PD.Endpoints, "127.0.0.1:2379")
	require.NoError(t, config.valid())

	config = Config{}
	_, localFile, _, _ := runtime.Caller(0)
	configFile := path.Join(path.Dir(localFile), "config.toml.example")
	require.NoError(t, config.Load(configFile))
	require.Equal(t, config.Addr, "0.0.0.0:8428")
	require.Equal(t, config.PD, PD{Endpoints: []string{"127.0.0.1:2379"}})
	require.Equal(t, config.Log, Log{Path: "log", Level: "INFO"})
	require.Equal(t, config.Storage, Storage{Path: "data"})
}
