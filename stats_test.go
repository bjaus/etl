package etl_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bjaus/etl"
)

func TestStats_NewStats(t *testing.T) {
	stats := etl.NewStats(100, 10, 95, 90, 5)
	require.Equal(t, int64(100), stats.Extracted())
	require.Equal(t, int64(10), stats.Filtered())
	require.Equal(t, int64(95), stats.Transformed())
	require.Equal(t, int64(90), stats.Loaded())
	require.Equal(t, int64(5), stats.Errors())
}

func TestStats_MarshalJSON(t *testing.T) {
	stats := etl.NewStats(100, 10, 95, 90, 5)
	data, err := stats.MarshalJSON()
	require.NoError(t, err)
	require.JSONEq(t, `{"extracted":100,"filtered":10,"transformed":95,"loaded":90,"errors":5}`, string(data))
}

func TestStats_UnmarshalJSON_Error(t *testing.T) {
	stats := &etl.Stats{}
	err := stats.UnmarshalJSON([]byte(`invalid json`))
	require.Error(t, err)
}
