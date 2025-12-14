package downloader

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// downloadAndExtract downloads a tar.gz file from the given URL and extracts
// it to the target directory. The extraction is done in a memory-efficient
// streaming fashion, piping the HTTP response directly through gzip decompression
// and tar extraction.
func downloadAndExtract(url, targetDir string) (int, error) {
	// Ensure target directory exists
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create target directory: %w", err)
	}

	// Download the file
	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected HTTP status: %d", resp.StatusCode)
	}

	// Create a gzip reader to decompress the stream
	gzipReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	// Create a tar reader to extract files
	tarReader := tar.NewReader(gzipReader)

	// Extract all files from the tar archive
	extractedCount := 0
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return extractedCount, fmt.Errorf("failed to read tar header: %w", err)
		}

		// Sanitize the tar entry name by removing leading slashes and cleaning the path
		// This prevents absolute paths and directory traversal attacks
		sanitizedName := strings.TrimPrefix(header.Name, "/")
		sanitizedName = strings.TrimPrefix(sanitizedName, "./")

		// Construct and clean the target path to normalize any double slashes or other issues
		targetPath := filepath.Clean(filepath.Join(targetDir, sanitizedName))

		// Security check: ensure the resolved path is within the target directory
		// This prevents directory traversal attacks (e.g., "../../etc/passwd")
		absTargetDir, err := filepath.Abs(targetDir)
		if err != nil {
			return extractedCount, fmt.Errorf("failed to resolve absolute path of target directory: %w", err)
		}
		absTargetPath, err := filepath.Abs(targetPath)
		if err != nil {
			return extractedCount, fmt.Errorf("failed to resolve absolute path: %w", err)
		}
		if !strings.HasPrefix(absTargetPath, absTargetDir+string(filepath.Separator)) && absTargetPath != absTargetDir {
			return extractedCount, fmt.Errorf("invalid path: %s is outside target directory", header.Name)
		}

		// Handle different types of entries
		switch header.Typeflag {
		case tar.TypeDir:
			if err := extractDirectory(targetPath, header.Mode); err != nil {
				return extractedCount, fmt.Errorf("failed to extract directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			if err := extractFile(tarReader, targetPath, header.Mode); err != nil {
				return extractedCount, fmt.Errorf("failed to extract file %s: %w", targetPath, err)
			}
			extractedCount++

		case tar.TypeSymlink:
			if err := extractSymlink(targetPath, header.Linkname); err != nil {
				return extractedCount, fmt.Errorf("failed to extract symlink %s: %w", targetPath, err)
			}

		default:
			// Skip unsupported entry types
		}
	}

	// Save version info after successful extraction
	etag := resp.Header.Get("ETag")
	if etag != "" {
		// Normalize ETag (remove quotes)
		etag = strings.Trim(etag, "\"")
		versionInfo := versionInfo{
			ETag: etag,
			URL:  url,
		}
		if err := saveVersionInfo(targetDir, versionInfo); err != nil {
			return extractedCount, fmt.Errorf("failed to save version info: %w", err)
		}
	}

	return extractedCount, nil
}

// extractDirectory creates a directory with the specified mode.
func extractDirectory(path string, mode int64) error {
	return os.MkdirAll(path, os.FileMode(mode))
}

// extractFile extracts a regular file from the tar reader to the target path.
func extractFile(tarReader *tar.Reader, targetPath string, mode int64) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	// Create the file
	outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(mode))
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer outFile.Close()

	// Copy file contents
	if _, err := io.Copy(outFile, tarReader); err != nil {
		return fmt.Errorf("failed to write file contents: %w", err)
	}

	return nil
}

// extractSymlink creates a symbolic link.
func extractSymlink(targetPath, linkname string) error {
	return os.Symlink(linkname, targetPath)
}

// removeDirectoryContents removes all contents of a directory but keeps the directory itself.
// This is useful when the directory was created with specific permissions/ownership that
// we want to preserve.
func removeDirectoryContents(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("failed to read directory: %w", err)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(dir, entry.Name())
		if err := os.RemoveAll(entryPath); err != nil {
			return fmt.Errorf("failed to remove %s: %w", entryPath, err)
		}
	}

	return nil
}

// versionInfo represents the version information stored in launcher-version.json
type versionInfo struct {
	ETag string `json:"etag"`
	URL  string `json:"url"`
}

// saveVersionInfo saves the version information to launcher-version.json
func saveVersionInfo(targetDir string, info versionInfo) error {
	versionPath := filepath.Join(targetDir, "launcher-version.json")
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal version info: %w", err)
	}
	if err := os.WriteFile(versionPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write version info file: %w", err)
	}
	return nil
}

// readVersionInfo reads the version information from launcher-version.json
func readVersionInfo(targetDir string) (*versionInfo, error) {
	versionPath := filepath.Join(targetDir, "launcher-version.json")
	data, err := os.ReadFile(versionPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No version file exists, which means we need to download
		}
		return nil, fmt.Errorf("failed to read version info file: %w", err)
	}

	var info versionInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal version info: %w", err)
	}

	// Normalize ETag (remove quotes)
	info.ETag = strings.Trim(info.ETag, "\"")

	return &info, nil
}

// GetETag performs a HEAD request to get the ETag header from the server.
func GetETag(url string) (string, error) {
	resp, err := http.Head(url)
	if err != nil {
		return "", fmt.Errorf("failed to perform HEAD request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected HTTP status: %d", resp.StatusCode)
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("no ETag header found in response")
	}

	// Remove quotes from ETag if present (ETags are often quoted)
	etag = strings.Trim(etag, "\"")

	return etag, nil
}

// NeedsDownload checks if a download is needed by comparing the server ETag and URL
// with the locally stored version info. Returns true if download is needed.
func NeedsDownload(url, targetDir string) (bool, error) {
	serverETag, err := GetETag(url)
	if err != nil {
		return true, err // If we can't get ETag, assume we need to download
	}

	localVersion, err := readVersionInfo(targetDir)
	if err != nil {
		return true, err // If we can't read local version info, assume we need to download
	}

	// If no local version info exists, we need to download
	if localVersion == nil {
		return true, nil
	}

	// Normalize server ETag for comparison
	serverETag = strings.Trim(serverETag, "\"")

	// Check if URL has changed
	if url != localVersion.URL {
		return true, nil
	}

	// Check if ETag has changed (case-insensitive comparison)
	if !strings.EqualFold(serverETag, localVersion.ETag) {
		return true, nil
	}

	// Both URL and ETag match, no download needed
	return false, nil
}

// DoServerBinaryDownload performs the complete server binary download process:
// checks for updates via ETag comparison, removes old binaries if needed,
// downloads and extracts the server binaries to the target directory.
// The URL is read from the VS_SERVER_TARGZ_URL environment variable.
func DoServerBinaryDownload(targetDir string) error {
	// Normalize and resolve the target directory path to handle any double slashes or other path issues
	// This ensures we always work with a clean, absolute path
	var err error
	targetDir, err = filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("failed to resolve absolute path: %w", err)
	}
	targetDir = filepath.Clean(targetDir)

	// Get the URL from environment variable
	url := os.Getenv("VS_SERVER_TARGZ_URL")
	if url == "" {
		return fmt.Errorf("VS_SERVER_TARGZ_URL environment variable is not set")
	}

	// Check if download is needed by comparing ETags
	fmt.Println("Checking for server binary updates...")
	needsDownload, err := NeedsDownload(url, targetDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: Failed to check ETag: %v\n", err)
		fmt.Println("Proceeding with download...")
		needsDownload = true
	}

	if !needsDownload {
		fmt.Println("Server binaries are up to date. Skipping download.")
		return nil
	}

	// If download is needed, remove existing directory contents (but keep the directory itself)
	// We keep the directory because it may have been created with specific permissions/ownership
	// (e.g., by root in a Dockerfile) that we can't recreate as a non-root user
	if _, err := os.Stat(targetDir); err == nil {
		fmt.Println("Removing existing server binaries...")
		if err := removeDirectoryContents(targetDir); err != nil {
			return fmt.Errorf("failed to remove existing directory contents: %w", err)
		}
	}

	fmt.Printf("Downloading Vintage Story server from %s...\n", url)
	fmt.Println("Extracting files...")

	extractedCount, err := downloadAndExtract(url, targetDir)
	if err != nil {
		return fmt.Errorf("failed to download and extract: %w", err)
	}

	fmt.Printf("Successfully extracted %d files to %s\n", extractedCount, targetDir)
	return nil
}
