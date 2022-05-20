package config

import (
	"path"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig(t *testing.T) {
	config := defaultConfig
	config.PD.Endpoints = append(config.PD.Endpoints, "10.0.1.8:2379")
	require.NoError(t, config.valid())
	require.Equal(t, config.PD, PD{Endpoints: []string{"10.0.1.8:2379"}})

	config = Config{}
	_, localFile, _, _ := runtime.Caller(0)
	configFile := path.Join(path.Dir(localFile), "config.toml.example")
	require.NoError(t, config.Load(configFile))
	require.Equal(t, config.Address, "0.0.0.0:12020")
	require.Equal(t, config.PD, PD{Endpoints: []string{"0.0.0.0:2379"}})
	require.Equal(t, config.Log, Log{Path: "log", Level: "INFO"})
	require.Equal(t, config.Storage, Storage{Path: "data"})
}

func TestContinueProfilingConfig(t *testing.T) {
	cfg := ContinueProfilingConfig{}
	require.Equal(t, cfg.Valid(), false)
	cfg = ContinueProfilingConfig{
		ProfileSeconds:       0,
		IntervalSeconds:      60,
		TimeoutSeconds:       120,
		DataRetentionSeconds: 10000,
	}
	require.Equal(t, cfg.Valid(), false)
	cfg.ProfileSeconds = 61
	require.Equal(t, cfg.Valid(), false)
	cfg.ProfileSeconds = 41
	cfg.TimeoutSeconds = 40
	require.Equal(t, cfg.Valid(), false)
	cfg = ContinueProfilingConfig{
		ProfileSeconds:       10,
		IntervalSeconds:      60,
		TimeoutSeconds:       120,
		DataRetentionSeconds: 10000,
	}
	require.Equal(t, cfg.Valid(), true)
}
