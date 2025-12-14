package backup

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		// Plain numbers (seconds)
		{name: "plain number", input: "60", expected: 60 * time.Second},
		{name: "plain number with spaces", input: "  30  ", expected: 30 * time.Second},
		{name: "zero", input: "0", expected: 0},

		// Seconds
		{name: "seconds suffix", input: "30s", expected: 30 * time.Second},
		{name: "seconds with space", input: "30 s", expected: 30 * time.Second},

		// Minutes
		{name: "minutes suffix", input: "5m", expected: 5 * time.Minute},
		{name: "one minute", input: "1m", expected: time.Minute},

		// Hours
		{name: "hours suffix", input: "2h", expected: 2 * time.Hour},
		{name: "one hour", input: "1h", expected: time.Hour},

		// Days
		{name: "days suffix", input: "1d", expected: 24 * time.Hour},
		{name: "two days", input: "2d", expected: 48 * time.Hour},

		// Weeks
		{name: "weeks suffix", input: "1w", expected: 7 * 24 * time.Hour},
		{name: "two weeks", input: "2w", expected: 14 * 24 * time.Hour},

		// Decimal values
		{name: "decimal seconds", input: "1.5s", expected: time.Duration(1.5 * float64(time.Second))},
		{name: "decimal hours", input: "0.5h", expected: 30 * time.Minute},
		{name: "decimal days", input: "0.5d", expected: 12 * time.Hour},

		// Error cases
		{name: "empty string", input: "", wantErr: true},
		{name: "only suffix", input: "s", wantErr: true},
		{name: "invalid number", input: "abc", wantErr: true},
		{name: "negative number", input: "-5s", wantErr: true},
		{name: "invalid suffix number", input: "foom", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseDuration(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ParseDuration(%q) expected error, got %v", tt.input, result)
				}
				return
			}

			if err != nil {
				t.Errorf("ParseDuration(%q) unexpected error: %v", tt.input, err)
				return
			}

			if result != tt.expected {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseDuration_LargeValues(t *testing.T) {
	// Test that large values work correctly
	result, err := ParseDuration("365d")
	if err != nil {
		t.Fatalf("ParseDuration(365d) unexpected error: %v", err)
	}

	expected := 365 * 24 * time.Hour
	if result != expected {
		t.Errorf("ParseDuration(365d) = %v, want %v", result, expected)
	}
}
