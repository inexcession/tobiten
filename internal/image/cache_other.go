//go:build !unix

package image

func canModifyDir(string) bool {
	return true
}
