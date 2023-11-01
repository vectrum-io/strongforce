package bus

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidSubjectPattern = errors.New("invalid subject pattern")
)

// same as in nats: https://github.com/nats-io/nats.go/blob/610da835da9546f9132cf4d566aa80d1e95ef93b/jetstream/jetstream.go#L216
var subjectRegexp = regexp.MustCompile(`^[^ >]*>?$`)

// MatchSubject checks whether a given subject matches a given pattern.
// The pattern can be expressed in the same syntax as in NATS.
// For more information about valid patterns check: https://docs.nats.io/nats-concepts/subjects#wildcards
func MatchSubject(subject, pattern string) bool {
	subjectTokens := strings.Split(subject, ".")
	patternTokens := strings.Split(pattern, ".")

	return matchTokens(subjectTokens, patternTokens)
}

func ValidatePattern(pattern string) error {
	if pattern == "" {
		return fmt.Errorf("%w: %s", ErrInvalidSubjectPattern, "pattern cannot be empty")
	}

	if !subjectRegexp.MatchString(pattern) {
		return fmt.Errorf("%w: %s", ErrInvalidSubjectPattern, pattern)
	}

	return nil
}

func matchTokens(subjectTokens, patternTokens []string) bool {
	// If there are no more tokens in the pattern, and no more tokens in the subject,
	// then it is a match.
	if len(patternTokens) == 0 && len(subjectTokens) == 0 {
		return true
	}

	// If there are no more tokens in the pattern, but there are still tokens in the subject,
	// then it is not a match.
	if len(patternTokens) == 0 {
		return false
	}

	// If there are more tokens in the pattern, but no more tokens in the subject,
	// then check if the remaining pattern token is ">", which means it's a match.
	if len(subjectTokens) == 0 {
		return patternTokens[0] == ">"
	}

	// If the pattern token is ">", it matches the remaining subject tokens.
	if patternTokens[0] == ">" {
		return true
	}

	// If the pattern token is "*", it matches any single subject token.
	if patternTokens[0] == "*" {
		return matchTokens(subjectTokens[1:], patternTokens[1:])
	}

	// If the current tokens are equal, continue with the next tokens.
	if subjectTokens[0] == patternTokens[0] {
		return matchTokens(subjectTokens[1:], patternTokens[1:])
	}

	// If none of the above conditions are met, it is not a match.
	return false
}
