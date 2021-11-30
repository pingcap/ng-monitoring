package testutil

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v3"
	"github.com/genjidb/genji"
	"github.com/genjidb/genji/engine/badgerengine"
	"github.com/stretchr/testify/require"
)

func NewGenjiDB(t *testing.T, storagePath string) *genji.DB {
	opts := badger.DefaultOptions(storagePath).
		WithZSTDCompressionLevel(3).
		WithBlockSize(8 * 1024).
		WithValueThreshold(128 * 1024)

	engine, err := badgerengine.NewEngine(opts)
	require.NoError(t, err)
	db, err := genji.New(context.Background(), engine)
	require.NoError(t, err)
	return db
}
