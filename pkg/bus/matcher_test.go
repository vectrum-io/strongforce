package bus

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInvalidPatterns(t *testing.T) {
	tests := []struct {
		pattern       string
		expectedError error
	}{
		{">", nil},
		{"foo.>", nil},
		{"", ErrInvalidSubjectPattern},
		{"foo.>bar", ErrInvalidSubjectPattern},
		{"foo.>.baz", ErrInvalidSubjectPattern},
		{"foo.>.>", ErrInvalidSubjectPattern},
	}

	for _, test := range tests {
		t.Run(test.pattern, func(t *testing.T) {
			err := ValidatePattern(test.pattern)
			if test.expectedError == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, test.expectedError)
			}
		})
	}
}

func TestMatch(t *testing.T) {
	tests := []struct {
		subject  string
		pattern  string
		expected bool
	}{
		{"foo.bar.baz", "foo.*.baz", true},
		{"foo.bar.baz.qux", "foo.>", true},
		{"foo.bar", "foo.*", true},
		{"foo.bar.baz", "foo.bar.baz", true},
		{"foo.bar.baz", "foo.bar.*", true},
		{"foo.bar.baz", "*.bar.baz", true},
		{"foo.bar.baz", "foo.>", true},
		{"foo.bar.baz", "foo.*.*", true},
		{"foo.bar.baz", "foo.*.>", true},
		{"foo.bar.baz", ">", true},
		{"foo", "*", true},
		// fail cases
		{"foo.bar.baz", "foo.*.*.baz", false},
		{"foo", "foo.bar", false},
		{"foo.bar.baz", "foo.*.*.*", false},
		{"foo.bar", "bar.>", false},
	}

	for _, test := range tests {
		t.Run(test.subject, func(t *testing.T) {
			result := MatchSubject(test.subject, test.pattern)
			if result != test.expected {
				t.Errorf("Match(%q, %q) = %v; expected %v", test.subject, test.pattern, result, test.expected)
			}
		})
	}
}
