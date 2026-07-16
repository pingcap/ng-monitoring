package logutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

func TestNewRotateWriter(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.log")
	writer, err := NewRotateWriter(log.FileLogConfig{
		Filename:   logFile,
		MaxSize:    10,
		MaxDays:    7,
		MaxBackups: 3,
	})
	require.NoError(t, err)
	require.Equal(t, logFile, writer.Filename)
	require.Equal(t, 10, writer.MaxSize)
	require.Equal(t, 7, writer.MaxAge)
	require.Equal(t, 3, writer.MaxBackups)

	info, err := os.Stat(logFile)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	require.NoError(t, writer.Close())
}

func TestNewRotateWriterFailsFast(t *testing.T) {
	_, err := NewRotateWriter(log.FileLogConfig{
		Filename: filepath.Join(t.TempDir(), "missing-dir", "test.log"),
	})
	require.Error(t, err)
}
