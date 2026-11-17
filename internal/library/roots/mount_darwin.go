//go:build darwin

package roots

import "golang.org/x/sys/unix"

// darwinPCCaseSensitive is _PC_CASE_SENSITIVE from <sys/unistd.h> on Darwin.
const darwinPCCaseSensitive = 11

func directoryLookupSemantics(dir string) lookupSemantics {
	// Darwin mounts (APFS, HFS+, SMB/AFP/foreign) normalize or may normalize
	// Unicode spellings on lookup; fail closed on default-ignorable code points
	// for unresolved suffixes while using _PC_CASE_SENSITIVE for case folding.
	val, err := unix.Pathconf(dir, darwinPCCaseSensitive)
	if err != nil || val < 0 {
		return lookupSemantics{
			caseInsensitive:        true,
			normInsensitive:        true,
			ignoreDefaultIgnorable: true,
		}
	}
	return lookupSemantics{
		caseInsensitive:        val == 0,
		normInsensitive:        true,
		ignoreDefaultIgnorable: true,
	}
}
