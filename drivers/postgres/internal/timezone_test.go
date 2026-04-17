package driver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolvePostgresTimeZone(t *testing.T) {
	t.Run("iana timezone", func(t *testing.T) {
		loc := resolvePostgresTimeZone("'Asia/Kolkata'")
		require.Equal(t, "Asia/Kolkata", loc.String())
	})

	t.Run("fixed offset", func(t *testing.T) {
		loc := resolvePostgresTimeZone("+05:30")
		_, offset := time.Now().In(loc).Zone()
		require.Equal(t, 19800, offset)
	})
}

func TestPostgresDataTypeConverterUsesEffectiveTimeZoneForNaiveTimestamp(t *testing.T) {
	loc := time.FixedZone("+05:30", 19800)
	p := &Postgres{effectiveTZ: loc}

	got, err := p.dataTypeConverter("2024-07-01 15:30:00", "timestamp without time zone")
	require.NoError(t, err)

	parsed, ok := got.(time.Time)
	require.True(t, ok)
	require.True(t, parsed.Equal(time.Date(2024, 7, 1, 15, 30, 0, 0, loc)))
	_, offset := parsed.Zone()
	require.Equal(t, 19800, offset)
}

func TestPostgresDataTypeConverterPreservesExplicitTimestampOffset(t *testing.T) {
	p := &Postgres{effectiveTZ: time.FixedZone("+05:30", 19800)}

	got, err := p.dataTypeConverter("2024-07-01 15:30:00+00", "timestamptz")
	require.NoError(t, err)

	parsed, ok := got.(time.Time)
	require.True(t, ok)
	require.True(t, parsed.Equal(time.Date(2024, 7, 1, 15, 30, 0, 0, time.UTC)))
}
