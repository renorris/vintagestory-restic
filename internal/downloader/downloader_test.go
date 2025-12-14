package downloader

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Helper function to create a tar.gz archive in memory for testing
func createTestTarGz(t *testing.T, files map[string]string, dirs []string, symlinks map[string]string) []byte {
	t.Helper()

	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzipWriter)

	// Add directories
	for _, dir := range dirs {
		header := &tar.Header{
			Name:     dir,
			Typeflag: tar.TypeDir,
			Mode:     0755,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("Failed to write dir header: %v", err)
		}
	}

	// Add files
	for name, content := range files {
		header := &tar.Header{
			Name:     name,
			Size:     int64(len(content)),
			Mode:     0644,
			Typeflag: tar.TypeReg,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("Failed to write file header: %v", err)
		}
		if _, err := tarWriter.Write([]byte(content)); err != nil {
			t.Fatalf("Failed to write file content: %v", err)
		}
	}

	// Add symlinks
	for name, target := range symlinks {
		header := &tar.Header{
			Name:     name,
			Typeflag: tar.TypeSymlink,
			Linkname: target,
		}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("Failed to write symlink header: %v", err)
		}
	}

	if err := tarWriter.Close(); err != nil {
		t.Fatalf("Failed to close tar writer: %v", err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("Failed to close gzip writer: %v", err)
	}

	return buf.Bytes()
}

func TestDownloadAndExtract_Success(t *testing.T) {
	// Create test tar.gz content
	files := map[string]string{
		"test1.txt":        "content1",
		"subdir/test2.txt": "content2",
		"executable.sh":    "#!/bin/bash\necho hello",
	}
	dirs := []string{"subdir/"}
	tarGzData := createTestTarGz(t, files, dirs, nil)

	// Create mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"test-etag-123\"")
		w.WriteHeader(http.StatusOK)
		w.Write(tarGzData)
	}))
	defer server.Close()

	// Create temporary directory for extraction
	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "extracted")

	// Test downloadAndExtract
	count, err := downloadAndExtract(server.URL, targetDir)
	if err != nil {
		t.Fatalf("downloadAndExtract failed: %v", err)
	}

	// Verify file count
	if count != 3 {
		t.Errorf("Expected 3 files extracted, got %d", count)
	}

	// Verify files were extracted correctly
	for name, expectedContent := range files {
		path := filepath.Join(targetDir, name)
		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read extracted file %s: %v", name, err)
			continue
		}
		if string(content) != expectedContent {
			t.Errorf("File %s: expected %q, got %q", name, expectedContent, string(content))
		}
	}

	// Verify version info was saved
	versionPath := filepath.Join(targetDir, "launcher-version.json")
	if _, err := os.Stat(versionPath); os.IsNotExist(err) {
		t.Error("Version info file was not created")
	} else {
		versionData, _ := os.ReadFile(versionPath)
		var info versionInfo
		json.Unmarshal(versionData, &info)
		if info.ETag != "test-etag-123" {
			t.Errorf("Version info ETag: expected %q, got %q", "test-etag-123", info.ETag)
		}
		if info.URL != server.URL {
			t.Errorf("Version info URL: expected %q, got %q", server.URL, info.URL)
		}
	}
}

func TestDownloadAndExtract_HTTPError(t *testing.T) {
	// Create mock HTTP server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	_, err := downloadAndExtract(server.URL, tmpDir)
	if err == nil {
		t.Fatal("Expected error for HTTP 404, got nil")
	}
	if !strings.Contains(err.Error(), "unexpected HTTP status: 404") {
		t.Errorf("Expected HTTP status error, got: %v", err)
	}
}

func TestDownloadAndExtract_InvalidGzip(t *testing.T) {
	// Create mock HTTP server that returns invalid gzip data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not a gzip file"))
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	_, err := downloadAndExtract(server.URL, tmpDir)
	if err == nil {
		t.Fatal("Expected error for invalid gzip, got nil")
	}
	if !strings.Contains(err.Error(), "failed to create gzip reader") {
		t.Errorf("Expected gzip error, got: %v", err)
	}
}

func TestDownloadAndExtract_WithSymlinks(t *testing.T) {
	files := map[string]string{
		"target.txt": "target content",
	}
	symlinks := map[string]string{
		"link.txt": "target.txt",
	}
	tarGzData := createTestTarGz(t, files, nil, symlinks)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"etag-with-symlinks\"")
		w.WriteHeader(http.StatusOK)
		w.Write(tarGzData)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "extracted")

	_, err := downloadAndExtract(server.URL, targetDir)
	if err != nil {
		t.Fatalf("downloadAndExtract with symlinks failed: %v", err)
	}

	// Verify symlink was created
	linkPath := filepath.Join(targetDir, "link.txt")
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("Failed to read symlink: %v", err)
	}
	if target != "target.txt" {
		t.Errorf("Symlink target: expected %q, got %q", "target.txt", target)
	}
}

func TestDownloadAndExtract_PathTraversalProtection(t *testing.T) {
	// Create tar with malicious path traversal attempts
	tests := []struct {
		name     string
		filename string
		wantErr  bool
	}{
		{
			name:     "absolute path",
			filename: "/etc/passwd",
			wantErr:  false, // Leading slash is stripped, so this becomes safe
		},
		{
			name:     "parent directory traversal",
			filename: "../../etc/passwd",
			wantErr:  true,
		},
		{
			name:     "complex traversal",
			filename: "subdir/../../etc/passwd",
			wantErr:  true,
		},
		{
			name:     "safe relative path",
			filename: "safe/file.txt",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files := map[string]string{
				tt.filename: "malicious content",
			}
			tarGzData := createTestTarGz(t, files, nil, nil)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write(tarGzData)
			}))
			defer server.Close()

			tmpDir := t.TempDir()
			targetDir := filepath.Join(tmpDir, "extracted")

			_, err := downloadAndExtract(server.URL, targetDir)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error for path traversal, got nil")
				} else if !strings.Contains(err.Error(), "outside target directory") {
					t.Errorf("Expected path traversal error, got: %v", err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestDownloadAndExtract_NoETag(t *testing.T) {
	files := map[string]string{
		"test.txt": "content",
	}
	tarGzData := createTestTarGz(t, files, nil, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No ETag header
		w.WriteHeader(http.StatusOK)
		w.Write(tarGzData)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "extracted")

	_, err := downloadAndExtract(server.URL, targetDir)
	if err != nil {
		t.Fatalf("downloadAndExtract failed: %v", err)
	}

	// Verify version info was not saved (or saved without ETag)
	versionPath := filepath.Join(targetDir, "launcher-version.json")
	if _, err := os.Stat(versionPath); err == nil {
		t.Error("Version info file should not be created without ETag")
	}
}

func TestExtractDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test", "nested", "dir")

	err := extractDirectory(testPath, 0755)
	if err != nil {
		t.Fatalf("extractDirectory failed: %v", err)
	}

	// Verify directory was created
	info, err := os.Stat(testPath)
	if err != nil {
		t.Fatalf("Directory was not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("Path is not a directory")
	}
}

func TestExtractFile(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "subdir", "test.txt")
	content := "test content"

	// Create a proper tar archive with test content
	var buf bytes.Buffer
	tarWriter := tar.NewWriter(&buf)
	header := &tar.Header{
		Name: "test.txt",
		Size: int64(len(content)),
		Mode: 0644,
	}
	tarWriter.WriteHeader(header)
	tarWriter.Write([]byte(content))
	tarWriter.Close()

	// Create tar reader
	tarReader := tar.NewReader(&buf)
	tarReader.Next() // Read the header

	err := extractFile(tarReader, testPath, 0644)
	if err != nil {
		t.Fatalf("extractFile failed: %v", err)
	}

	// Verify file was created with correct content
	readContent, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}
	if string(readContent) != content {
		t.Errorf("File content: expected %q, got %q", content, string(readContent))
	}
}

func TestExtractSymlink(t *testing.T) {
	tmpDir := t.TempDir()
	linkPath := filepath.Join(tmpDir, "link.txt")
	targetPath := "target.txt"

	err := extractSymlink(linkPath, targetPath)
	if err != nil {
		t.Fatalf("extractSymlink failed: %v", err)
	}

	// Verify symlink was created correctly
	readTarget, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("Failed to read symlink: %v", err)
	}
	if readTarget != targetPath {
		t.Errorf("Symlink target: expected %q, got %q", targetPath, readTarget)
	}
}

func TestSaveAndReadVersionInfo(t *testing.T) {
	tmpDir := t.TempDir()

	testInfo := versionInfo{
		ETag: "test-etag",
		URL:  "https://example.com/test.tar.gz",
	}

	// Test saving
	err := saveVersionInfo(tmpDir, testInfo)
	if err != nil {
		t.Fatalf("saveVersionInfo failed: %v", err)
	}

	// Verify file was created
	versionPath := filepath.Join(tmpDir, "launcher-version.json")
	if _, err := os.Stat(versionPath); os.IsNotExist(err) {
		t.Fatal("Version info file was not created")
	}

	// Test reading
	readInfo, err := readVersionInfo(tmpDir)
	if err != nil {
		t.Fatalf("readVersionInfo failed: %v", err)
	}

	if readInfo == nil {
		t.Fatal("readVersionInfo returned nil")
	}

	if readInfo.ETag != testInfo.ETag {
		t.Errorf("ETag: expected %q, got %q", testInfo.ETag, readInfo.ETag)
	}
	if readInfo.URL != testInfo.URL {
		t.Errorf("URL: expected %q, got %q", testInfo.URL, readInfo.URL)
	}
}

func TestReadVersionInfo_NotExist(t *testing.T) {
	tmpDir := t.TempDir()

	info, err := readVersionInfo(tmpDir)
	if err != nil {
		t.Errorf("Expected no error for missing file, got: %v", err)
	}
	if info != nil {
		t.Error("Expected nil for missing version info")
	}
}

func TestReadVersionInfo_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	versionPath := filepath.Join(tmpDir, "launcher-version.json")

	// Write invalid JSON
	err := os.WriteFile(versionPath, []byte("not valid json"), 0644)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	_, err = readVersionInfo(tmpDir)
	if err == nil {
		t.Error("Expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "failed to unmarshal") {
		t.Errorf("Expected unmarshal error, got: %v", err)
	}
}

func TestReadVersionInfo_ETagNormalization(t *testing.T) {
	tmpDir := t.TempDir()
	versionPath := filepath.Join(tmpDir, "launcher-version.json")

	// Write version info with quoted ETag
	data := `{"etag": "\"quoted-etag\"", "url": "https://example.com"}`
	err := os.WriteFile(versionPath, []byte(data), 0644)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	info, err := readVersionInfo(tmpDir)
	if err != nil {
		t.Fatalf("readVersionInfo failed: %v", err)
	}

	// Verify ETag quotes were removed
	if info.ETag != "quoted-etag" {
		t.Errorf("ETag quotes not normalized: got %q", info.ETag)
	}
}

func TestGetETag_Success(t *testing.T) {
	expectedETag := "test-etag-456"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("Expected HEAD request, got %s", r.Method)
		}
		w.Header().Set("ETag", "\""+expectedETag+"\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	etag, err := GetETag(server.URL)
	if err != nil {
		t.Fatalf("GetETag failed: %v", err)
	}

	if etag != expectedETag {
		t.Errorf("ETag: expected %q, got %q", expectedETag, etag)
	}
}

func TestGetETag_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := GetETag(server.URL)
	if err == nil {
		t.Fatal("Expected error for HTTP 404")
	}
	if !strings.Contains(err.Error(), "unexpected HTTP status: 404") {
		t.Errorf("Expected HTTP status error, got: %v", err)
	}
}

func TestGetETag_NoETagHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No ETag header
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	_, err := GetETag(server.URL)
	if err == nil {
		t.Fatal("Expected error for missing ETag header")
	}
	if !strings.Contains(err.Error(), "no ETag header found") {
		t.Errorf("Expected ETag header error, got: %v", err)
	}
}

func TestGetETag_ETagNormalization(t *testing.T) {
	tests := []struct {
		name         string
		serverETag   string
		expectedETag string
	}{
		{
			name:         "quoted etag",
			serverETag:   "\"abc123\"",
			expectedETag: "abc123",
		},
		{
			name:         "unquoted etag",
			serverETag:   "abc123",
			expectedETag: "abc123",
		},
		{
			name:         "weak etag",
			serverETag:   "W/\"abc123\"",
			expectedETag: "W/\"abc123", // strings.Trim only removes leading/trailing quotes
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", tt.serverETag)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			etag, err := GetETag(server.URL)
			if err != nil {
				t.Fatalf("GetETag failed: %v", err)
			}

			if etag != tt.expectedETag {
				t.Errorf("ETag: expected %q, got %q", tt.expectedETag, etag)
			}
		})
	}
}

func TestNeedsDownload_NoLocalVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"new-etag\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	needs, err := NeedsDownload(server.URL, tmpDir)
	if err != nil {
		t.Fatalf("NeedsDownload failed: %v", err)
	}

	if !needs {
		t.Error("Expected download needed when no local version exists")
	}
}

func TestNeedsDownload_SameETagAndURL(t *testing.T) {
	etag := "same-etag"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\""+etag+"\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	// Save version info
	info := versionInfo{
		ETag: etag,
		URL:  server.URL,
	}
	saveVersionInfo(tmpDir, info)

	needs, err := NeedsDownload(server.URL, tmpDir)
	if err != nil {
		t.Fatalf("NeedsDownload failed: %v", err)
	}

	if needs {
		t.Error("Expected no download needed when ETag and URL match")
	}
}

func TestNeedsDownload_DifferentETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"new-etag\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	// Save old version info
	info := versionInfo{
		ETag: "old-etag",
		URL:  server.URL,
	}
	saveVersionInfo(tmpDir, info)

	needs, err := NeedsDownload(server.URL, tmpDir)
	if err != nil {
		t.Fatalf("NeedsDownload failed: %v", err)
	}

	if !needs {
		t.Error("Expected download needed when ETag differs")
	}
}

func TestNeedsDownload_DifferentURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"same-etag\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	// Save version info with different URL
	info := versionInfo{
		ETag: "same-etag",
		URL:  "https://different-url.com/file.tar.gz",
	}
	saveVersionInfo(tmpDir, info)

	needs, err := NeedsDownload(server.URL, tmpDir)
	if err != nil {
		t.Fatalf("NeedsDownload failed: %v", err)
	}

	if !needs {
		t.Error("Expected download needed when URL differs")
	}
}

func TestNeedsDownload_CaseInsensitiveETag(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "\"ABC123\"")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	// Save version info with lowercase etag
	info := versionInfo{
		ETag: "abc123",
		URL:  server.URL,
	}
	saveVersionInfo(tmpDir, info)

	needs, err := NeedsDownload(server.URL, tmpDir)
	if err != nil {
		t.Fatalf("NeedsDownload failed: %v", err)
	}

	if needs {
		t.Error("Expected no download needed (case-insensitive ETag comparison)")
	}
}

func TestNeedsDownload_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	tmpDir := t.TempDir()

	needs, err := NeedsDownload(server.URL, tmpDir)
	if err == nil {
		t.Fatal("Expected error when server returns error")
	}

	// Should still return true (needs download) even with error
	if !needs {
		t.Error("Expected needs=true when ETag check fails")
	}
}

func TestDoServerBinaryDownload_MissingEnvVar(t *testing.T) {
	// Save and unset env var
	oldURL := os.Getenv("VS_SERVER_TARGZ_URL")
	os.Unsetenv("VS_SERVER_TARGZ_URL")
	defer func() {
		if oldURL != "" {
			os.Setenv("VS_SERVER_TARGZ_URL", oldURL)
		}
	}()

	tmpDir := t.TempDir()

	err := DoServerBinaryDownload(tmpDir)
	if err == nil {
		t.Fatal("Expected error when VS_SERVER_TARGZ_URL not set")
	}
	if !strings.Contains(err.Error(), "VS_SERVER_TARGZ_URL environment variable is not set") {
		t.Errorf("Expected env var error, got: %v", err)
	}
}

func TestDoServerBinaryDownload_Success(t *testing.T) {
	files := map[string]string{
		"server.exe": "server binary",
		"data.json":  "{}",
	}
	tarGzData := createTestTarGz(t, files, nil, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", "\"test-etag\"")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("ETag", "\"test-etag\"")
		w.WriteHeader(http.StatusOK)
		w.Write(tarGzData)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "server")

	// Set environment variable
	oldURL := os.Getenv("VS_SERVER_TARGZ_URL")
	os.Setenv("VS_SERVER_TARGZ_URL", server.URL)
	defer func() {
		if oldURL != "" {
			os.Setenv("VS_SERVER_TARGZ_URL", oldURL)
		} else {
			os.Unsetenv("VS_SERVER_TARGZ_URL")
		}
	}()

	err := DoServerBinaryDownload(targetDir)
	if err != nil {
		t.Fatalf("DoServerBinaryDownload failed: %v", err)
	}

	// Verify files were extracted
	for name, expectedContent := range files {
		path := filepath.Join(targetDir, name)
		content, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("Failed to read file %s: %v", name, err)
			continue
		}
		if string(content) != expectedContent {
			t.Errorf("File %s: expected %q, got %q", name, expectedContent, string(content))
		}
	}
}

func TestDoServerBinaryDownload_SkipsWhenUpToDate(t *testing.T) {
	etag := "unchanged-etag"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", "\""+etag+"\"")
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Error("Should not perform GET request when up to date")
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "server")

	// Create target directory and save version info
	os.MkdirAll(targetDir, 0755)
	info := versionInfo{
		ETag: etag,
		URL:  server.URL,
	}
	saveVersionInfo(targetDir, info)

	// Create a test file to verify directory is not removed
	testFile := filepath.Join(targetDir, "existing.txt")
	os.WriteFile(testFile, []byte("should remain"), 0644)

	// Set environment variable
	oldURL := os.Getenv("VS_SERVER_TARGZ_URL")
	os.Setenv("VS_SERVER_TARGZ_URL", server.URL)
	defer func() {
		if oldURL != "" {
			os.Setenv("VS_SERVER_TARGZ_URL", oldURL)
		} else {
			os.Unsetenv("VS_SERVER_TARGZ_URL")
		}
	}()

	err := DoServerBinaryDownload(targetDir)
	if err != nil {
		t.Fatalf("DoServerBinaryDownload failed: %v", err)
	}

	// Verify existing file is still there
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatal("Existing file was removed")
	}
	if string(content) != "should remain" {
		t.Error("Existing file was modified")
	}
}

func TestDoServerBinaryDownload_RemovesOldFiles(t *testing.T) {
	files := map[string]string{
		"new-file.txt": "new content",
	}
	tarGzData := createTestTarGz(t, files, nil, nil)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", "\"new-etag\"")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("ETag", "\"new-etag\"")
		w.WriteHeader(http.StatusOK)
		w.Write(tarGzData)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "server")

	// Create target directory with old files
	os.MkdirAll(targetDir, 0755)
	oldFile := filepath.Join(targetDir, "old-file.txt")
	os.WriteFile(oldFile, []byte("old content"), 0644)

	// Save old version info
	info := versionInfo{
		ETag: "old-etag",
		URL:  server.URL,
	}
	saveVersionInfo(targetDir, info)

	// Set environment variable
	oldURL := os.Getenv("VS_SERVER_TARGZ_URL")
	os.Setenv("VS_SERVER_TARGZ_URL", server.URL)
	defer func() {
		if oldURL != "" {
			os.Setenv("VS_SERVER_TARGZ_URL", oldURL)
		} else {
			os.Unsetenv("VS_SERVER_TARGZ_URL")
		}
	}()

	err := DoServerBinaryDownload(targetDir)
	if err != nil {
		t.Fatalf("DoServerBinaryDownload failed: %v", err)
	}

	// Verify old file is gone
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("Old file was not removed")
	}

	// Verify new file exists
	newFile := filepath.Join(targetDir, "new-file.txt")
	if _, err := os.Stat(newFile); err != nil {
		t.Error("New file was not created")
	}
}

func TestDoServerBinaryDownload_PathNormalization(t *testing.T) {
	// Save original env
	originalURL := os.Getenv("VS_SERVER_TARGZ_URL")
	defer func() {
		if originalURL != "" {
			os.Setenv("VS_SERVER_TARGZ_URL", originalURL)
		} else {
			os.Unsetenv("VS_SERVER_TARGZ_URL")
		}
	}()

	// Test with double slash path
	testDir := "//test-serverbinaries"

	// This should fail due to missing URL, but path should be normalized first
	os.Unsetenv("VS_SERVER_TARGZ_URL")
	err := DoServerBinaryDownload(testDir)

	// Should get error about missing URL, not about path
	if err == nil {
		t.Fatal("Expected error for missing VS_SERVER_TARGZ_URL")
	}

	// Verify the error is about URL, not path issues
	if err.Error() != "VS_SERVER_TARGZ_URL environment variable is not set" {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestDoServerBinaryDownload_ContinuesOnETagCheckFailure(t *testing.T) {
	files := map[string]string{
		"file.txt": "content",
	}
	tarGzData := createTestTarGz(t, files, nil, nil)

	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if r.Method == http.MethodHead {
			// Fail HEAD request
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		// But allow GET request
		w.Header().Set("ETag", "\"etag\"")
		w.WriteHeader(http.StatusOK)
		w.Write(tarGzData)
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	targetDir := filepath.Join(tmpDir, "server")

	oldURL := os.Getenv("VS_SERVER_TARGZ_URL")
	os.Setenv("VS_SERVER_TARGZ_URL", server.URL)
	defer func() {
		if oldURL != "" {
			os.Setenv("VS_SERVER_TARGZ_URL", oldURL)
		} else {
			os.Unsetenv("VS_SERVER_TARGZ_URL")
		}
	}()

	err := DoServerBinaryDownload(targetDir)
	if err != nil {
		t.Fatalf("DoServerBinaryDownload should succeed despite ETag check failure: %v", err)
	}

	// Verify file was still downloaded
	if _, err := os.Stat(filepath.Join(targetDir, "file.txt")); err != nil {
		t.Error("File was not downloaded despite ETag check failure")
	}
}

func TestPathNormalization(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "double slash at start",
			input:    "//serverbinaries",
			expected: filepath.Join("/", "serverbinaries"),
		},
		{
			name:     "single slash",
			input:    "/serverbinaries",
			expected: filepath.Join("/", "serverbinaries"),
		},
		{
			name:     "relative path",
			input:    "./serverbinaries",
			expected: filepath.Join(mustAbs("."), "serverbinaries"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalized, err := filepath.Abs(tt.input)
			if err != nil {
				t.Fatalf("filepath.Abs failed: %v", err)
			}
			normalized = filepath.Clean(normalized)

			expectedAbs, err := filepath.Abs(tt.expected)
			if err != nil {
				t.Fatalf("filepath.Abs failed for expected: %v", err)
			}
			expectedAbs = filepath.Clean(expectedAbs)

			if normalized != expectedAbs {
				t.Errorf("Path normalization failed: got %q, want %q", normalized, expectedAbs)
			}

			// Verify no double slashes in the result
			if containsDoubleSlash(normalized) {
				t.Errorf("Normalized path contains double slash: %q", normalized)
			}
		})
	}
}

func TestDirectoryRemovalWithNormalizedPath(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	// Create a subdirectory that simulates the problematic scenario
	testSubDir := filepath.Join(tmpDir, "serverbinaries")
	if err := os.MkdirAll(testSubDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create a file in the directory to ensure it's not empty
	testFile := filepath.Join(testSubDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test that we can remove the directory using a path that might have double slashes
	// Simulate the scenario where targetDir might be passed with double slashes
	testPath := filepath.Join(tmpDir, "//serverbinaries")

	// Normalize the path (as DoServerBinaryDownload does)
	normalizedPath, err := filepath.Abs(testPath)
	if err != nil {
		t.Fatalf("filepath.Abs failed: %v", err)
	}
	normalizedPath = filepath.Clean(normalizedPath)

	// Verify the normalized path matches the actual directory
	if normalizedPath != testSubDir {
		t.Errorf("Path normalization mismatch: got %q, want %q", normalizedPath, testSubDir)
	}

	// Verify we can remove it using the normalized path
	if err := os.RemoveAll(normalizedPath); err != nil {
		t.Fatalf("Failed to remove directory with normalized path: %v", err)
	}

	// Verify the directory is actually gone
	if _, err := os.Stat(testSubDir); !os.IsNotExist(err) {
		t.Errorf("Directory should have been removed, but still exists")
	}
}

// Helper functions

func mustAbs(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	return abs
}

func containsDoubleSlash(path string) bool {
	for i := 0; i < len(path)-1; i++ {
		if path[i] == '/' && path[i+1] == '/' {
			return true
		}
	}
	return false
}
