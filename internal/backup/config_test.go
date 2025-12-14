package backup

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name           string
		envValue       string
		expectEnabled  bool
		expectInterval time.Duration
		expectErr      bool
	}{
		{
			name:          "not set",
			envValue:      "",
			expectEnabled: false,
		},
		{
			name:           "valid seconds",
			envValue:       "60",
			expectEnabled:  true,
			expectInterval: 60 * time.Second,
		},
		{
			name:           "valid minutes",
			envValue:       "5m",
			expectEnabled:  true,
			expectInterval: 5 * time.Minute,
		},
		{
			name:           "valid hours",
			envValue:       "1h",
			expectEnabled:  true,
			expectInterval: time.Hour,
		},
		{
			name:           "valid days",
			envValue:       "1d",
			expectEnabled:  true,
			expectInterval: 24 * time.Hour,
		},
		{
			name:           "valid weeks",
			envValue:       "1w",
			expectEnabled:  true,
			expectInterval: 7 * 24 * time.Hour,
		},
		{
			name:      "invalid format",
			envValue:  "invalid",
			expectErr: true,
		},
		{
			name:      "zero duration",
			envValue:  "0",
			expectErr: true,
		},
		{
			name:      "negative duration",
			envValue:  "-5s",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set or unset environment variable
			if tt.envValue == "" {
				os.Unsetenv("BACKUP_INTERVAL")
			} else {
				os.Setenv("BACKUP_INTERVAL", tt.envValue)
			}
			defer os.Unsetenv("BACKUP_INTERVAL")
			defer os.Unsetenv("DO_BACKUP_ON_SERVER_START")

			config, err := LoadConfig()

			if tt.expectErr {
				if err == nil {
					t.Error("LoadConfig() expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("LoadConfig() unexpected error: %v", err)
			}

			if config.Enabled != tt.expectEnabled {
				t.Errorf("LoadConfig().Enabled = %v, want %v", config.Enabled, tt.expectEnabled)
			}

			if tt.expectEnabled && config.Interval != tt.expectInterval {
				t.Errorf("LoadConfig().Interval = %v, want %v", config.Interval, tt.expectInterval)
			}
		})
	}
}

func TestLoadConfig_BackupOnServerStart(t *testing.T) {
	tests := []struct {
		name                      string
		backupOnStartEnv          string
		expectBackupOnServerStart bool
	}{
		{
			name:                      "not set",
			backupOnStartEnv:          "",
			expectBackupOnServerStart: false,
		},
		{
			name:                      "true",
			backupOnStartEnv:          "true",
			expectBackupOnServerStart: true,
		},
		{
			name:                      "TRUE uppercase",
			backupOnStartEnv:          "TRUE",
			expectBackupOnServerStart: true,
		},
		{
			name:                      "True mixed case",
			backupOnStartEnv:          "True",
			expectBackupOnServerStart: true,
		},
		{
			name:                      "1",
			backupOnStartEnv:          "1",
			expectBackupOnServerStart: true,
		},
		{
			name:                      "yes",
			backupOnStartEnv:          "yes",
			expectBackupOnServerStart: true,
		},
		{
			name:                      "YES uppercase",
			backupOnStartEnv:          "YES",
			expectBackupOnServerStart: true,
		},
		{
			name:                      "false",
			backupOnStartEnv:          "false",
			expectBackupOnServerStart: false,
		},
		{
			name:                      "0",
			backupOnStartEnv:          "0",
			expectBackupOnServerStart: false,
		},
		{
			name:                      "no",
			backupOnStartEnv:          "no",
			expectBackupOnServerStart: false,
		},
		{
			name:                      "invalid value",
			backupOnStartEnv:          "invalid",
			expectBackupOnServerStart: false,
		},
		{
			name:                      "whitespace around true",
			backupOnStartEnv:          "  true  ",
			expectBackupOnServerStart: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set backup interval to enable backups
			os.Setenv("BACKUP_INTERVAL", "1h")
			defer os.Unsetenv("BACKUP_INTERVAL")

			if tt.backupOnStartEnv == "" {
				os.Unsetenv("DO_BACKUP_ON_SERVER_START")
			} else {
				os.Setenv("DO_BACKUP_ON_SERVER_START", tt.backupOnStartEnv)
			}
			defer os.Unsetenv("DO_BACKUP_ON_SERVER_START")

			config, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() unexpected error: %v", err)
			}

			if config.BackupOnServerStart != tt.expectBackupOnServerStart {
				t.Errorf("LoadConfig().BackupOnServerStart = %v, want %v", config.BackupOnServerStart, tt.expectBackupOnServerStart)
			}
		})
	}
}

func TestParseBoolEnv(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"1", true},
		{"yes", true},
		{"YES", true},
		{"Yes", true},
		{"false", false},
		{"FALSE", false},
		{"0", false},
		{"no", false},
		{"", false},
		{"invalid", false},
		{"  true  ", true},
		{"  1  ", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseBoolEnv(tt.input)
			if result != tt.expected {
				t.Errorf("parseBoolEnv(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestLoadConfig_PauseWhenNoPlayers(t *testing.T) {
	tests := []struct {
		name                     string
		pauseEnv                 string
		expectPauseWhenNoPlayers bool
	}{
		{
			name:                     "not set",
			pauseEnv:                 "",
			expectPauseWhenNoPlayers: false,
		},
		{
			name:                     "true",
			pauseEnv:                 "true",
			expectPauseWhenNoPlayers: true,
		},
		{
			name:                     "1",
			pauseEnv:                 "1",
			expectPauseWhenNoPlayers: true,
		},
		{
			name:                     "yes",
			pauseEnv:                 "yes",
			expectPauseWhenNoPlayers: true,
		},
		{
			name:                     "false",
			pauseEnv:                 "false",
			expectPauseWhenNoPlayers: false,
		},
		{
			name:                     "0",
			pauseEnv:                 "0",
			expectPauseWhenNoPlayers: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("BACKUP_INTERVAL", "1h")
			defer os.Unsetenv("BACKUP_INTERVAL")

			if tt.pauseEnv == "" {
				os.Unsetenv("BACKUP_PAUSE_WHEN_NO_PLAYERS")
			} else {
				os.Setenv("BACKUP_PAUSE_WHEN_NO_PLAYERS", tt.pauseEnv)
			}
			defer os.Unsetenv("BACKUP_PAUSE_WHEN_NO_PLAYERS")

			config, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() unexpected error: %v", err)
			}

			if config.PauseWhenNoPlayers != tt.expectPauseWhenNoPlayers {
				t.Errorf("LoadConfig().PauseWhenNoPlayers = %v, want %v", config.PauseWhenNoPlayers, tt.expectPauseWhenNoPlayers)
			}
		})
	}
}

func TestValidateResticEnv(t *testing.T) {
	tests := []struct {
		name           string
		repository     string
		password       string
		expectErr      bool
		expectedErrMsg string
	}{
		{
			name:       "both set",
			repository: "s3:s3.amazonaws.com/bucket",
			password:   "secret123",
			expectErr:  false,
		},
		{
			name:           "repository missing",
			repository:     "",
			password:       "secret123",
			expectErr:      true,
			expectedErrMsg: "RESTIC_REPOSITORY",
		},
		{
			name:           "password missing",
			repository:     "s3:s3.amazonaws.com/bucket",
			password:       "",
			expectErr:      true,
			expectedErrMsg: "RESTIC_PASSWORD",
		},
		{
			name:           "both missing",
			repository:     "",
			password:       "",
			expectErr:      true,
			expectedErrMsg: "RESTIC_REPOSITORY", // Should fail on first check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set or unset environment variables
			if tt.repository == "" {
				os.Unsetenv("RESTIC_REPOSITORY")
			} else {
				os.Setenv("RESTIC_REPOSITORY", tt.repository)
			}
			defer os.Unsetenv("RESTIC_REPOSITORY")

			if tt.password == "" {
				os.Unsetenv("RESTIC_PASSWORD")
			} else {
				os.Setenv("RESTIC_PASSWORD", tt.password)
			}
			defer os.Unsetenv("RESTIC_PASSWORD")

			err := ValidateResticEnv()

			if tt.expectErr {
				if err == nil {
					t.Error("ValidateResticEnv() expected error, got nil")
					return
				}
				if tt.expectedErrMsg != "" && !strings.Contains(err.Error(), tt.expectedErrMsg) {
					t.Errorf("ValidateResticEnv() error message should contain %q, got %q", tt.expectedErrMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("ValidateResticEnv() unexpected error: %v", err)
				}
			}
		})
	}
}
