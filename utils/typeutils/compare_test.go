package typeutils

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompare_Nil(t *testing.T) {
	assert.Equal(t, 0, Compare(nil, nil))
	assert.Equal(t, -1, Compare(nil, 1))
	assert.Equal(t, 1, Compare(1, nil))
}

func TestCompare_SignedIntegers(t *testing.T) {
	assert.Equal(t, 0, Compare(int64(5), int(5)))
	assert.Equal(t, -1, Compare(int(-1), int(1)))
	assert.Equal(t, 1, Compare(int32(10), int8(2)))
}

func TestCompare_UnsignedIntegers(t *testing.T) {
	assert.Equal(t, 0, Compare(uint64(5), uint(5)))
	assert.Equal(t, -1, Compare(uint32(1), uint8(2)))
	assert.Equal(t, 1, Compare(uint(8), uint16(1)))
}

func TestCompare_Floats(t *testing.T) {
	assert.Equal(t, 0, Compare(float64(3.3), float32(3.3)))
	assert.Equal(t, -1, Compare(float32(1.1), float64(2.2)))
	assert.Equal(t, 1, Compare(float64(5.5), float64(1.1)))
}

func TestCompare_FloatEdgeCases(t *testing.T) {
	assert.Equal(t, 0, Compare(math.NaN(), math.NaN()))
	assert.Equal(t, -1, Compare(math.NaN(), 1.0))
	assert.Equal(t, 1, Compare(1.0, math.NaN()))

	assert.Equal(t, 1, Compare(math.Inf(1), math.Inf(1)))
	assert.Equal(t, 1, Compare(math.Inf(1), 1.0))
	assert.Equal(t, -1, Compare(math.Inf(-1), 1.0))
}

func TestCompare_Time(t *testing.T) {
	a := time.Now()
	b := time.Now().Add(time.Hour)
	assert.Equal(t, 0, Compare(a, a))
	assert.Equal(t, -1, Compare(a, b))
	assert.Equal(t, 1, Compare(b, a))
}

func TestCompare_Bool(t *testing.T) {
	assert.Equal(t, 0, Compare(false, false))
	assert.Equal(t, 0, Compare(true, true))
	assert.Equal(t, -1, Compare(false, true))
	assert.Equal(t, 1, Compare(true, false))
}

func TestCompare_Fallback(t *testing.T) {
	assert.Equal(t, 0, Compare("123", 123))
	assert.Equal(t, -1, Compare("apple", "banana"))
	assert.Equal(t, 1, Compare(struct{ A int }{2}, struct{ A int }{1}))
}
