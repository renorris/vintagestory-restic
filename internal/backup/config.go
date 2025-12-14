package backup

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds the backup configuration parsed from environment variables.
type Config struct {
	// Enabled indicates whether backups are enabled.
	Enabled bool

	// Interval is the time between backups.
	Interval time.Duration

	// BackupOnServerStart indicates whether a backup should be performed
	// immediately when the server finishes booting.
	BackupOnServerStart bool
}

// LoadConfig loads backup configuration from environment variables.
// Returns a Config with Enabled=false if BACKUP_INTERVAL is not set.
func LoadConfig() (*Config, error) {
	intervalStr := os.Getenv("BACKUP_INTERVAL")
	if intervalStr == "" {
		return &Config{Enabled: false}, nil
	}

	interval, err := ParseDuration(intervalStr)
	if err != nil {
		return nil, fmt.Errorf("invalid BACKUP_INTERVAL: %w", err)
	}

	if interval <= 0 {
		return nil, fmt.Errorf("BACKUP_INTERVAL must be positive, got %v", interval)
	}

	backupOnStart := parseBoolEnv(os.Getenv("DO_BACKUP_ON_SERVER_START"))

	return &Config{
		Enabled:             true,
		Interval:            interval,
		BackupOnServerStart: backupOnStart,
	}, nil
}

// parseBoolEnv parses a boolean from an environment variable string.
// Returns true for "true", "1", "yes" (case-insensitive), false otherwise.
func parseBoolEnv(s string) bool {
	s = strings.ToLower(strings.TrimSpace(s))
	return s == "true" || s == "1" || s == "yes"
}

// ValidateResticEnv validates that required restic environment variables are set
// when backups are enabled. Returns an error if any required variables are missing.
func ValidateResticEnv() error {
	if os.Getenv("RESTIC_REPOSITORY") == "" {
		return fmt.Errorf("FATAL: BACKUP_INTERVAL is set but RESTIC_REPOSITORY is not set. Backups require RESTIC_REPOSITORY to be configured")
	}
	if os.Getenv("RESTIC_PASSWORD") == "" {
		return fmt.Errorf("FATAL: BACKUP_INTERVAL is set but RESTIC_PASSWORD is not set. Backups require RESTIC_PASSWORD to be configured")
	}
	return nil
}
