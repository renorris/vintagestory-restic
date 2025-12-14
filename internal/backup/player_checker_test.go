package backup

import (
	"testing"
)

func TestPlayerChecker_HandleOutput_DetectsPlayerJoin(t *testing.T) {
	pc := &PlayerChecker{}

	// Player joins
	pc.HandleOutput("[Server Event] amoglaswag joins.")

	if !pc.PlayersOnline() {
		t.Error("PlayersOnline() = false, want true after player join")
	}

	if pc.PlayerCount() != 1 {
		t.Errorf("PlayerCount() = %d, want 1", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_DetectsPlayerLeave(t *testing.T) {
	pc := &PlayerChecker{}

	// Player joins then leaves
	pc.HandleOutput("[Server Event] amoglaswag joins.")
	pc.HandleOutput("[Server Event] amoglaswag left.")

	if pc.PlayersOnline() {
		t.Error("PlayersOnline() = true, want false after player left")
	}

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_MultiplePlayers(t *testing.T) {
	pc := &PlayerChecker{}

	// Multiple players join
	pc.HandleOutput("[Server Event] player1 joins.")
	pc.HandleOutput("[Server Event] player2 joins.")
	pc.HandleOutput("[Server Event] player3 joins.")

	if pc.PlayerCount() != 3 {
		t.Errorf("PlayerCount() = %d, want 3", pc.PlayerCount())
	}

	// One leaves
	pc.HandleOutput("[Server Event] player2 left.")

	if pc.PlayerCount() != 2 {
		t.Errorf("PlayerCount() = %d, want 2 after one left", pc.PlayerCount())
	}

	if !pc.PlayersOnline() {
		t.Error("PlayersOnline() = false, want true with 2 players remaining")
	}
}

func TestPlayerChecker_HandleOutput_DoesNotGoNegative(t *testing.T) {
	pc := &PlayerChecker{}

	// Leave without join (shouldn't happen, but be safe)
	pc.HandleOutput("[Server Event] ghost left.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 (should not go negative)", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_IgnoresOtherServerEvents(t *testing.T) {
	pc := &PlayerChecker{}

	// Other server events should be ignored
	pc.HandleOutput("[Server Event] World loaded")
	pc.HandleOutput("[Server Event] Chunk saved")
	pc.HandleOutput("[Server Event] Day 5 started")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 for non-join/leave events", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_IgnoresOtherLogLines(t *testing.T) {
	pc := &PlayerChecker{}

	// Non-server-event lines should be ignored
	pc.HandleOutput("[Chat] player1: hello world")
	pc.HandleOutput("[Notification] List of online Players")
	pc.HandleOutput("14.12.2025 21:32:37 [Event] Something happened")
	pc.HandleOutput("[2] amoglaswag [::ffff:10.88.0.79]:47300")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 for non-server-event lines", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_PlayerNamesWithSpaces(t *testing.T) {
	pc := &PlayerChecker{}

	// Player name with spaces
	pc.HandleOutput("[Server Event] Some Player Name joins.")

	if pc.PlayerCount() != 1 {
		t.Errorf("PlayerCount() = %d, want 1 for player with spaces in name", pc.PlayerCount())
	}

	pc.HandleOutput("[Server Event] Some Player Name left.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 after player with spaces left", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_PlayerNamesWithSpecialChars(t *testing.T) {
	pc := &PlayerChecker{}

	// Player name with special characters
	pc.HandleOutput("[Server Event] Player_123.test joins.")

	if pc.PlayerCount() != 1 {
		t.Errorf("PlayerCount() = %d, want 1 for player with special chars", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_TimestampPrefix(t *testing.T) {
	pc := &PlayerChecker{}

	// Lines may have timestamp prefix in logs
	pc.HandleOutput("14.12.2025 21:32:37 [Server Event] player1 joins.")

	if pc.PlayerCount() != 1 {
		t.Errorf("PlayerCount() = %d, want 1 for line with timestamp prefix", pc.PlayerCount())
	}

	pc.HandleOutput("14.12.2025 21:35:00 [Server Event] player1 left.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 after player left with timestamp prefix", pc.PlayerCount())
	}
}

// Tests for the single [Server Event] security check
func TestPlayerChecker_HandleOutput_IgnoresMultipleServerEventMarkers(t *testing.T) {
	pc := &PlayerChecker{}

	// Attempt to spoof join with multiple [Server Event] markers
	// This could be an attack vector where someone tries to inject fake events
	pc.HandleOutput("[Server Event] [Server Event] attacker joins.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore lines with multiple [Server Event] markers", pc.PlayerCount())
	}

	// Another attempt with [Server Event] appearing multiple times
	pc.HandleOutput("[Server Event] Someone said [Server Event] and then left.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore lines with [Server Event] appearing twice", pc.PlayerCount())
	}

	// Nested attempt
	pc.HandleOutput("14.12.2025 [Server Event] [Server Event] player joins.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore lines with multiple markers even with timestamp", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_IgnoresZeroServerEventMarkers(t *testing.T) {
	pc := &PlayerChecker{}

	// Lines without [Server Event] should be ignored
	pc.HandleOutput("player1 joins.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore lines without [Server Event]", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_IgnoresChatMessages(t *testing.T) {
	pc := &PlayerChecker{}

	// Chat messages containing [Server Event] patterns should be ignored
	// This prevents players from typing fake join/leave messages in chat
	pc.HandleOutput("[Server Chat] attacker: [Server Event] fakeplayer joins.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore chat messages", pc.PlayerCount())
	}

	// Chat message with left pattern
	pc.HandleOutput("[Server Chat] attacker: [Server Event] fakeplayer left.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore chat left messages", pc.PlayerCount())
	}

	// Chat message with timestamp
	pc.HandleOutput("14.12.2025 21:32:37 [Server Chat] attacker: [Server Event] fakeplayer joins.")

	if pc.PlayerCount() != 0 {
		t.Errorf("PlayerCount() = %d, want 0 - should ignore timestamped chat messages", pc.PlayerCount())
	}

	// Verify that valid join still works after chat is ignored
	pc.HandleOutput("[Server Event] realplayer joins.")

	if pc.PlayerCount() != 1 {
		t.Errorf("PlayerCount() = %d, want 1 - valid join should still work", pc.PlayerCount())
	}
}

func TestPlayerChecker_HandleOutput_AcceptsExactlyOneServerEventMarker(t *testing.T) {
	pc := &PlayerChecker{}

	// Exactly one [Server Event] marker is valid
	pc.HandleOutput("[Server Event] player1 joins.")

	if pc.PlayerCount() != 1 {
		t.Errorf("PlayerCount() = %d, want 1 - should accept lines with exactly one [Server Event]", pc.PlayerCount())
	}
}

// Tests for ShouldBackup() method
func TestPlayerChecker_ShouldBackup_PlayersOnline(t *testing.T) {
	pc := &PlayerChecker{}

	// Player joins
	pc.HandleOutput("[Server Event] player1 joins.")

	// Should backup because players are online
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true when players are online")
	}

	// Should still backup on subsequent checks while players online
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true on subsequent check with players online")
	}
}

func TestPlayerChecker_ShouldBackup_NoPlayersNeverWere(t *testing.T) {
	pc := &PlayerChecker{}

	// No players ever joined
	if pc.ShouldBackup() {
		t.Error("ShouldBackup() = true, want false when no players ever online")
	}

	// Still no backup needed
	if pc.ShouldBackup() {
		t.Error("ShouldBackup() = true, want false on subsequent check with no players")
	}
}

func TestPlayerChecker_ShouldBackup_FinalBackupWhenPlayersLeave(t *testing.T) {
	pc := &PlayerChecker{}

	// Player joins
	pc.HandleOutput("[Server Event] player1 joins.")

	// First check - players online
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true when players are online")
	}

	// Player leaves
	pc.HandleOutput("[Server Event] player1 left.")

	// Second check - should trigger final backup since players WERE online before
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true for final backup when players just left")
	}

	// Third check - no players, and no players last time either
	if pc.ShouldBackup() {
		t.Error("ShouldBackup() = true, want false - final backup already triggered")
	}
}

func TestPlayerChecker_ShouldBackup_MultiplePlayersLeaveGradually(t *testing.T) {
	pc := &PlayerChecker{}

	// Two players join
	pc.HandleOutput("[Server Event] player1 joins.")
	pc.HandleOutput("[Server Event] player2 joins.")

	// First check - players online
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true when players are online")
	}

	// One player leaves (one still online)
	pc.HandleOutput("[Server Event] player1 left.")

	// Second check - still players online
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true when one player still online")
	}

	// Last player leaves
	pc.HandleOutput("[Server Event] player2 left.")

	// Third check - final backup triggered
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true for final backup")
	}

	// Fourth check - no more backups needed
	if pc.ShouldBackup() {
		t.Error("ShouldBackup() = true, want false - final backup already triggered")
	}
}

func TestPlayerChecker_ShouldBackup_PlayerRejoins(t *testing.T) {
	pc := &PlayerChecker{}

	// Player joins
	pc.HandleOutput("[Server Event] player1 joins.")
	pc.ShouldBackup() // players online = true

	// Player leaves
	pc.HandleOutput("[Server Event] player1 left.")
	pc.ShouldBackup() // final backup triggered

	// Player rejoins
	pc.HandleOutput("[Server Event] player1 joins.")

	// Should backup again because player is back online
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true when player rejoins")
	}

	// Player leaves again
	pc.HandleOutput("[Server Event] player1 left.")

	// Final backup should trigger again
	if !pc.ShouldBackup() {
		t.Error("ShouldBackup() = false, want true for another final backup")
	}
}

func TestPlayerJoinPatternMatching(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		matches bool
	}{
		{
			name:    "valid join",
			input:   "[Server Event] player1 joins.",
			matches: true,
		},
		{
			name:    "valid join with timestamp",
			input:   "14.12.2025 21:32:37 [Server Event] player1 joins.",
			matches: true,
		},
		{
			name:    "valid join with spaces in name",
			input:   "[Server Event] Some Cool Player joins.",
			matches: true,
		},
		{
			name:    "join without period",
			input:   "[Server Event] player1 joins",
			matches: false,
		},
		{
			name:    "different event type",
			input:   "[Event] player1 joins.",
			matches: false,
		},
		{
			name:    "chat message mentioning joins",
			input:   "[Chat] player: someone joins.",
			matches: false,
		},
		{
			name:    "left event - not a join",
			input:   "[Server Event] player1 left.",
			matches: false,
		},
		{
			name:    "empty line",
			input:   "",
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := playerJoinPattern.MatchString(tt.input)
			if result != tt.matches {
				t.Errorf("playerJoinPattern.MatchString(%q) = %v, want %v", tt.input, result, tt.matches)
			}
		})
	}
}

func TestPlayerLeavePatternMatching(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		matches bool
	}{
		{
			name:    "valid leave",
			input:   "[Server Event] player1 left.",
			matches: true,
		},
		{
			name:    "valid leave with timestamp",
			input:   "14.12.2025 21:32:37 [Server Event] player1 left.",
			matches: true,
		},
		{
			name:    "valid leave with spaces in name",
			input:   "[Server Event] Some Cool Player left.",
			matches: true,
		},
		{
			name:    "leave without period",
			input:   "[Server Event] player1 left",
			matches: false,
		},
		{
			name:    "different event type",
			input:   "[Event] player1 left.",
			matches: false,
		},
		{
			name:    "chat message mentioning left",
			input:   "[Chat] player: someone left.",
			matches: false,
		},
		{
			name:    "join event - not a leave",
			input:   "[Server Event] player1 joins.",
			matches: false,
		},
		{
			name:    "empty line",
			input:   "",
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := playerLeavePattern.MatchString(tt.input)
			if result != tt.matches {
				t.Errorf("playerLeavePattern.MatchString(%q) = %v, want %v", tt.input, result, tt.matches)
			}
		})
	}
}

func TestPlayerChecker_Concurrency(t *testing.T) {
	pc := &PlayerChecker{}

	// Simulate concurrent join/leave events
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			pc.HandleOutput("[Server Event] player1 joins.")
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 100; i++ {
			pc.HandleOutput("[Server Event] player2 joins.")
		}
		done <- struct{}{}
	}()

	<-done
	<-done

	if pc.PlayerCount() != 200 {
		t.Errorf("PlayerCount() = %d, want 200 after concurrent joins", pc.PlayerCount())
	}
}
