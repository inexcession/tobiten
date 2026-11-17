//go:build !darwin && !linux

package roots

func directoryLookupSemantics(_ string) lookupSemantics {
	return lookupSemantics{
		caseInsensitive:        true,
		normInsensitive:        true,
		ignoreDefaultIgnorable: true,
	}
}
