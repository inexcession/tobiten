//go:build !darwin && !linux

package mount

func statfsInfo(_ string) (FilesystemInfo, error) {
	return FilesystemInfo{
		Type:      "unknown",
		IsNetwork: false,
	}, nil
}
