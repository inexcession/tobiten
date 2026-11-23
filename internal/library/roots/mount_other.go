//go:build !darwin && !linux

package roots

import "os"

func directoryLookupSemantics(_ string) lookupSemantics {
	return lookupSemantics{
		caseInsensitive:        true,
		normInsensitive:        true,
		ignoreDefaultIgnorable: true,
	}
}

func sameFileIdentityAndChangeTime(a, b os.FileInfo) bool {
	return os.SameFile(a, b)
}
