package scrape

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTicker(t *testing.T) {
	ticker := NewTicker(time.Millisecond * 50)
	defer ticker.Stop()
	tc := ticker.Subscribe()
	require.Equal(t, 1, len(ticker.subscribers))
	tc.Stop()
	require.Equal(t, 0, len(ticker.subscribers))

	tc1 := ticker.Subscribe()
	tc2 := ticker.Subscribe()
	t1 := <-tc1.ch
	t2 := <-tc2.ch
	require.Equal(t, t1.Unix(), t2.Unix())

	ticker.Reset(time.Millisecond * 70)
	t1 = <-tc1.ch
	t2 = <-tc2.ch
	require.Equal(t, t1.Unix(), t2.Unix())
}
