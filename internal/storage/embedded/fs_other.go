//go:build !darwin && !linux

package embedded

func statfsInfo(path string) (FilesystemInfo, error) {
	return FilesystemInfo{
		Type:      "unknown",
		IsNetwork: false,
	}, nil
}
