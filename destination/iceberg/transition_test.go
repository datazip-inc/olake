package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsValidTransitionBinary pins how binary columns evolve: bytes detected from values fit an
// existing fixed[n] column, fixed bytes and text both fit an existing binary column (String sits
// under Binary in the typecast tree), but a fixed[n] column cannot become variable-length or
// text, and a text column cannot become binary.
func TestIsValidTransitionBinary(t *testing.T) {
	assert.True(t, isValidTransition("binary", "binary"))
	assert.True(t, isValidTransition("fixed[16]", "fixed[16]"))
	assert.True(t, isValidTransition("fixed[16]", "binary"), "value detection cannot know a width")
	assert.True(t, isValidTransition("binary", "fixed[16]"), "fixed bytes fit a binary column")
	assert.False(t, isValidTransition("fixed[16]", "fixed[32]"))
	assert.False(t, isValidTransition("string", "binary"))
	assert.True(t, isValidTransition("binary", "string"), "text fits a binary column as its bytes")
	assert.False(t, isValidTransition("fixed[16]", "string"))
	assert.False(t, isPromotionRequired("fixed[16]", "binary"))
}
