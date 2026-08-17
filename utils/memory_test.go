package utils

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sizeOfNode struct {
	next *sizeOfNode
}

type sizeOfBox struct {
	inner *int
}

func TestSizeOf(t *testing.T) {
	t.Run("fixed width kinds report their type size", func(t *testing.T) {
		assert.Equal(t, 1, SizeOf(true))
		assert.Equal(t, 1, SizeOf(int8(1)))
		assert.Equal(t, 2, SizeOf(int16(1)))
		assert.Equal(t, 4, SizeOf(int32(1)))
		assert.Equal(t, 8, SizeOf(int64(1)))
		assert.Equal(t, 8, SizeOf(float64(1)))
	})

	t.Run("string grows with its bytes", func(t *testing.T) {
		assert.Equal(t, 3, SizeOf("abcdef")-SizeOf("abc"))
	})

	t.Run("array counts its elements", func(t *testing.T) {
		assert.Equal(t, 24, SizeOf([3]int64{1, 2, 3}))
	})

	t.Run("interface carries its header", func(t *testing.T) {
		assert.Greater(t, SizeOf([]any{int64(1)}), SizeOf([]int64{1}))
	})

	t.Run("slice grows with its elements", func(t *testing.T) {
		assert.Equal(t, 16, SizeOf([]int64{1, 2, 3})-SizeOf([]int64{1}))
	})

	t.Run("spare slice capacity is counted", func(t *testing.T) {
		assert.Greater(t, SizeOf(make([]int64, 1, 4)), SizeOf(make([]int64, 1)))
	})

	t.Run("nested pointer is followed", func(t *testing.T) {
		value := 7
		assert.Greater(t, SizeOf(sizeOfBox{inner: &value}), SizeOf(sizeOfBox{}))
	})

	t.Run("pointer cycle terminates", func(t *testing.T) {
		node := &sizeOfNode{}
		node.next = node
		assert.Greater(t, SizeOf(node), 0)
	})

	t.Run("the same pointer is only counted once", func(t *testing.T) {
		value := 7
		shared := struct{ a, b *int }{&value, &value}
		distinct := struct{ a, b *int }{&value, new(int)}
		assert.Less(t, SizeOf(shared), SizeOf(distinct))
	})

	t.Run("map counts keys and values", func(t *testing.T) {
		assert.Greater(t, SizeOf(map[string]int64{"a": 1, "b": 2}), SizeOf(map[string]int64{"a": 1}))
	})

	t.Run("unsupported value reports -1", func(t *testing.T) {
		require.Equal(t, -1, SizeOf(nil))
	})
}

func TestDetermineSystemMemoryGB(t *testing.T) {
	got := DetermineSystemMemoryGB()
	if runtime.GOOS == "linux" || runtime.GOOS == "darwin" {
		assert.Greater(t, got, int64(0), "expected the host to report its memory")
		return
	}
	// elsewhere the probe may not exist, but it still has to answer -1 rather than a bogus size
	assert.True(t, got == -1 || got > 0)
}
