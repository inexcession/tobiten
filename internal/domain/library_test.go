package domain

import (
	"bytes"
	"errors"
	"slices"
	"testing"
	"time"
)

func mustRootIdentity(t *testing.T, raw []byte) PathIdentity {
	t.Helper()
	id, err := NewRootPathIdentity(raw)
	if err != nil {
		t.Fatalf("NewRootPathIdentity(%x) failed: %v", raw, err)
	}
	return id
}

func TestParseLibraryMode(t *testing.T) {
	cases := []struct {
		raw     string
		want    LibraryMode
		wantErr error
	}{
		{raw: "virtual", want: LibraryModeVirtual},
		{raw: "VIRTUAL", want: LibraryModeVirtual},
		{raw: " managed ", want: LibraryModeManaged},
		{raw: "MANAGED", want: LibraryModeManaged},
		{raw: "", wantErr: ErrInvalidLibraryMode},
		{raw: "hybrid", wantErr: ErrInvalidLibraryMode},
	}

	for _, tc := range cases {
		got, err := ParseLibraryMode(tc.raw)
		if !errors.Is(err, tc.wantErr) {
			t.Errorf("ParseLibraryMode(%q) err = %v, want %v", tc.raw, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("ParseLibraryMode(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestValidateResolverChain(t *testing.T) {
	validChains := [][]ResolverName{
		DefaultResolverChain(),
		{ResolverFolder, ResolverMetadata, ResolverFilename},
		{ResolverMetadata},
		{ResolverFolder, ResolverFilename},
	}
	for _, chain := range validChains {
		if err := ValidateResolverChain(chain); err != nil {
			t.Errorf("ValidateResolverChain(%v) unexpected error: %v", chain, err)
		}
	}

	invalidCases := []struct {
		name    string
		chain   []ResolverName
		wantErr error
	}{
		{
			name:    "nil chain",
			chain:   nil,
			wantErr: ErrEmptyResolverChain,
		},
		{
			name:    "empty chain",
			chain:   []ResolverName{},
			wantErr: ErrEmptyResolverChain,
		},
		{
			name:    "unknown resolver",
			chain:   []ResolverName{ResolverMetadata, ResolverName("scraper")},
			wantErr: ErrInvalidResolverName,
		},
		{
			name:    "empty resolver element",
			chain:   []ResolverName{ResolverName("")},
			wantErr: ErrInvalidResolverName,
		},
		{
			name:    "duplicate resolver",
			chain:   []ResolverName{ResolverMetadata, ResolverFolder, ResolverMetadata},
			wantErr: ErrDuplicateResolverName,
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateResolverChain(tc.chain)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("ValidateResolverChain(%v) = %v, want %v", tc.chain, err, tc.wantErr)
			}
		})
	}
}

func TestLibraryValidate(t *testing.T) {
	intake := mustRootIdentity(t, []byte("/mnt/intake"))
	invalidUTF8Intake := mustRootIdentity(t, []byte{'/', 'm', 'n', 't', '/', 0xff, 0xfe})

	baseVirtual := func() Library {
		return Library{
			ID:                 "lib-1",
			Name:               "Manga",
			Mode:               LibraryModeVirtual,
			ResolverChain:      DefaultResolverChain(),
			MetadataPrecedence: MetadataPrecedenceEmbeddedOverOnline,
			DefaultCoverPolicy: CoverPolicyFirst,
		}
	}

	tests := []struct {
		name    string
		build   func() *Library
		wantErr error
	}{
		{
			name: "valid virtual library",
			build: func() *Library {
				l := baseVirtual()
				return &l
			},
		},
		{
			name: "valid managed library with intake",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryModeManaged
				l.IntakePath = &intake
				l.AutoMetadataWriteback = true
				l.ResolverChain = []ResolverName{ResolverFolder, ResolverMetadata}
				l.MetadataPrecedence = MetadataPrecedenceOnlineOverEmbedded
				l.DefaultCoverPolicy = CoverPolicyLatest
				return &l
			},
		},
		{
			name: "valid managed library with invalid UTF-8 intake",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryModeManaged
				l.IntakePath = &invalidUTF8Intake
				return &l
			},
		},
		{
			name: "virtual library rejects intake path",
			build: func() *Library {
				l := baseVirtual()
				l.IntakePath = &intake
				return &l
			},
			wantErr: ErrVirtualLibraryWithIntake,
		},
		{
			name: "managed library requires intake path",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryModeManaged
				return &l
			},
			wantErr: ErrManagedLibraryRequiresIntake,
		},
		{
			name: "managed library rejects partial intake path without display/match",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryModeManaged
				l.IntakePath = &PathIdentity{Bytes: []byte("/mnt/intake")}
				return &l
			},
			wantErr: ErrInvalidPathIdentity,
		},
		{
			name: "managed library rejects mismatched intake match key",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryModeManaged
				l.IntakePath = &PathIdentity{
					Bytes:   []byte("/mnt/Cafe\u0301"),
					Display: "/mnt/Cafe\u0301",
					Match:   "/mnt/Cafe\u0301", // not NFC
				}
				return &l
			},
			wantErr: ErrInvalidPathIdentity,
		},
		{
			name: "managed library rejects relative intake path",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryModeManaged
				l.IntakePath = &PathIdentity{
					Bytes:   []byte("intake"),
					Display: "intake",
					Match:   "intake",
				}
				return &l
			},
			wantErr: ErrInvalidRootPath,
		},
		{
			name: "empty library id",
			build: func() *Library {
				l := baseVirtual()
				l.ID = "  "
				return &l
			},
			wantErr: ErrEmptyLibraryID,
		},
		{
			name: "empty library name",
			build: func() *Library {
				l := baseVirtual()
				l.Name = "   "
				return &l
			},
			wantErr: ErrEmptyLibraryName,
		},
		{
			name: "invalid mode",
			build: func() *Library {
				l := baseVirtual()
				l.Mode = LibraryMode("READONLY")
				return &l
			},
			wantErr: ErrInvalidLibraryMode,
		},
		{
			name: "unknown resolver in chain",
			build: func() *Library {
				l := baseVirtual()
				l.ResolverChain = []ResolverName{ResolverMetadata, ResolverName("bogus")}
				return &l
			},
			wantErr: ErrInvalidResolverName,
		},
		{
			name: "duplicate resolver in chain",
			build: func() *Library {
				l := baseVirtual()
				l.ResolverChain = []ResolverName{ResolverFolder, ResolverFolder}
				return &l
			},
			wantErr: ErrDuplicateResolverName,
		},
		{
			name: "invalid metadata precedence",
			build: func() *Library {
				l := baseVirtual()
				l.MetadataPrecedence = MetadataPrecedence("INVALID")
				return &l
			},
			wantErr: ErrInvalidMetadataPrecedence,
		},
		{
			name: "invalid cover policy",
			build: func() *Library {
				l := baseVirtual()
				l.DefaultCoverPolicy = CoverPolicy("INVALID")
				return &l
			},
			wantErr: ErrInvalidCoverPolicy,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.build().Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Library.Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestLibraryRootValidate(t *testing.T) {
	now := time.Now().UTC()
	lastErr := "mount unreadable"
	validPath := mustRootIdentity(t, []byte("/mnt/manga"))
	invalidUTF8Path := mustRootIdentity(t, []byte{'/', 'm', 'n', 't', '/', 0xff, 'a'})

	tests := []struct {
		name    string
		root    *LibraryRoot
		wantErr error
	}{
		{
			name: "valid online root",
			root: &LibraryRoot{
				ID:        "root-1",
				LibraryID: "lib-1",
				Path:      validPath,
				State:     LibraryRootStateOnline,
			},
		},
		{
			name: "valid offline root with invalid UTF-8 path and status details",
			root: &LibraryRoot{
				ID:            "root-2",
				LibraryID:     "lib-1",
				Path:          invalidUTF8Path,
				State:         LibraryRootStateOffline,
				LastCheckedAt: &now,
				LastError:     &lastErr,
			},
		},
		{
			name: "empty root id",
			root: &LibraryRoot{
				LibraryID: "lib-1",
				Path:      validPath,
				State:     LibraryRootStateOnline,
			},
			wantErr: ErrEmptyLibraryRootID,
		},
		{
			name: "empty library id",
			root: &LibraryRoot{
				ID:    "root-1",
				Path:  validPath,
				State: LibraryRootStateOnline,
			},
			wantErr: ErrEmptyLibraryID,
		},
		{
			name: "relative root path rejected",
			root: &LibraryRoot{
				ID:        "root-1",
				LibraryID: "lib-1",
				Path: PathIdentity{
					Bytes:   []byte("relative/path"),
					Display: "relative/path",
					Match:   "relative/path",
				},
				State: LibraryRootStateOnline,
			},
			wantErr: ErrInvalidRootPath,
		},
		{
			name: "incomplete path identity rejected",
			root: &LibraryRoot{
				ID:        "root-1",
				LibraryID: "lib-1",
				Path:      PathIdentity{Bytes: []byte("/mnt/manga")},
				State:     LibraryRootStateOnline,
			},
			wantErr: ErrInvalidPathIdentity,
		},
		{
			name: "invalid root state",
			root: &LibraryRoot{
				ID:        "root-1",
				LibraryID: "lib-1",
				Path:      validPath,
				State:     LibraryRootState("MISSING"),
			},
			wantErr: ErrInvalidLibraryRootState,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.root.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("LibraryRoot.Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestClonePathIdentity(t *testing.T) {
	raw := []byte{'/', 'm', 'n', 't', '/', 0xff}
	orig := mustRootIdentity(t, raw)
	cloned := ClonePathIdentity(orig)
	orig.Bytes[1] = 'X'

	if !bytes.Equal(cloned.Bytes, []byte{'/', 'm', 'n', 't', '/', 0xff}) {
		t.Fatalf("ClonePathIdentity did not isolate Bytes: %x", cloned.Bytes)
	}
	if !cloned.InvalidUTF8 {
		t.Fatalf("expected InvalidUTF8=true on cloned invalid-UTF-8 path")
	}
	if !slices.Equal(ValidMetadataPrecedences(), []MetadataPrecedence{
		MetadataPrecedenceEmbeddedOverOnline,
		MetadataPrecedenceOnlineOverEmbedded,
	}) {
		t.Fatal("unexpected ValidMetadataPrecedences")
	}
	if !slices.Equal(ValidLibraryRootStates(), []LibraryRootState{
		LibraryRootStateOnline,
		LibraryRootStateOffline,
	}) {
		t.Fatal("unexpected ValidLibraryRootStates")
	}
}
