package human

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestSize(t *testing.T) {
	tests := []struct {
		size      int64
		formatted string
	}{
		{1, "1B"},
		{1501, "1.5K"},
		{1600 * 1024, "1.6M"},
		{1100 * 1024 * 1024, "1.1G"},
		{1300 * 1024 * 1024 * 1024, "1.3T"},
		{9990 * 1024 * 1024 * 1024 * 1024, "9.8P"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.formatted, Size(tt.size))
	}
}
