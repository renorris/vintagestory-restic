// Package backup provides periodic backup functionality for Vintage Story servers.
// It handles backup scheduling, staging directory creation with copy-on-write,
// and integration with restic for efficient incremental backups.
package backup

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseDuration parses a duration string in a flexible format.
// If just a number is provided, it's interpreted as seconds.
// Supported suffixes: s (seconds), m (minutes), h (hours), d (days), w (weeks).
//
// Examples:
//   - "60" -> 60 seconds
//   - "60s" -> 60 seconds
//   - "5m" -> 5 minutes
//   - "2h" -> 2 hours
//   - "1d" -> 1 day
//   - "1w" -> 1 week
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration string")
	}

	// Check for our custom suffixes first
	var multiplier time.Duration
	var numStr string

	if len(s) > 0 {
		lastChar := s[len(s)-1]
		switch lastChar {
		case 's':
			multiplier = time.Second
			numStr = s[:len(s)-1]
		case 'm':
			multiplier = time.Minute
			numStr = s[:len(s)-1]
		case 'h':
			multiplier = time.Hour
			numStr = s[:len(s)-1]
		case 'd':
			multiplier = 24 * time.Hour
			numStr = s[:len(s)-1]
		case 'w':
			multiplier = 7 * 24 * time.Hour
			numStr = s[:len(s)-1]
		default:
			// No suffix - treat as seconds
			multiplier = time.Second
			numStr = s
		}
	}

	// Parse the numeric part
	numStr = strings.TrimSpace(numStr)
	if numStr == "" {
		return 0, fmt.Errorf("invalid duration: missing numeric value")
	}

	// Try parsing as float to support decimals
	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration number %q: %w", numStr, err)
	}

	if num < 0 {
		return 0, fmt.Errorf("duration cannot be negative")
	}

	return time.Duration(num * float64(multiplier)), nil
}
