package domain

import (
	"errors"
	"testing"
)

func TestCreditRole_AllTenValid(t *testing.T) {
	roles := ValidCreditRoles()
	if len(roles) != 10 {
		t.Fatalf("len(ValidCreditRoles()) = %d, want 10", len(roles))
	}
	want := map[CreditRole]bool{
		CreditRoleWriter:      true,
		CreditRoleArtist:      true,
		CreditRolePenciller:   true,
		CreditRoleInker:       true,
		CreditRoleColorist:    true,
		CreditRoleLetterer:    true,
		CreditRoleCoverArtist: true,
		CreditRoleEditor:      true,
		CreditRoleTranslator:  true,
		CreditRoleAuthor:      true,
	}
	for _, r := range roles {
		if !r.IsValid() {
			t.Errorf("role %q reported invalid", r)
		}
		if !want[r] {
			t.Errorf("unexpected role %q", r)
		}
		delete(want, r)
	}
	if len(want) != 0 {
		t.Errorf("missing roles: %v", want)
	}
	if CreditRole("DIRECTOR").IsValid() {
		t.Error("expected unknown CreditRole to be invalid")
	}
}

func TestEntityKind_AllThreeValid(t *testing.T) {
	kinds := ValidEntityKinds()
	if len(kinds) != 3 {
		t.Fatalf("len(ValidEntityKinds()) = %d, want 3", len(kinds))
	}
	want := map[EntityKind]bool{
		EntityKindCharacter: true,
		EntityKindTeam:      true,
		EntityKindLocation:  true,
	}
	for _, k := range kinds {
		if !k.IsValid() {
			t.Errorf("kind %q reported invalid", k)
		}
		if !want[k] {
			t.Errorf("unexpected kind %q", k)
		}
		delete(want, k)
	}
	if len(want) != 0 {
		t.Errorf("missing kinds: %v", want)
	}
	if EntityKind("EVENT").IsValid() {
		t.Error("expected unknown EntityKind to be invalid")
	}
}

func TestNormalizeImportName(t *testing.T) {
	cases := []struct {
		name string
		a    string
		b    string
		same bool
	}{
		{
			name: "ascii case folding",
			a:    "Eiichiro Oda",
			b:    "EIICHIRO ODA",
			same: true,
		},
		{
			name: "unicode whitespace trimming",
			a:    "\u3000\u00a0  Stan Lee \t\n",
			b:    "stan lee",
			same: true,
		},
		{
			name: "NFC vs NFD equivalence with case",
			a:    "\u00c9lodie",
			b:    "e\u0301LODIE",
			same: true,
		},
		{
			name: "sharp s unicode case folding",
			a:    "Weiß",
			b:    "WEISS",
			same: true,
		},
		{
			name: "ligature fi unicode case folding",
			a:    "\ufb01ona",
			b:    "FIONA",
			same: true,
		},
		{
			name: "greek final sigma",
			a:    "Σ",
			b:    "ς",
			same: true,
		},
		{
			name: "non-cased scripts preserved",
			a:    "尾田栄一郎",
			b:    "  尾田栄一郎\u3000",
			same: true,
		},
		{
			name: "distinct names stay distinct",
			a:    "Eiichiro Oda",
			b:    "Naoki Urasawa",
			same: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ka := NormalizeImportName(tc.a)
			kb := NormalizeImportName(tc.b)
			if ka == "" || kb == "" {
				t.Fatalf("expected non-empty normalized keys, got %q and %q", ka, kb)
			}
			if (ka == kb) != tc.same {
				t.Fatalf("NormalizeImportName(%q)=%q vs NormalizeImportName(%q)=%q, same=%v want %v",
					tc.a, ka, tc.b, kb, ka == kb, tc.same)
			}
			if NormalizeImportName(ka) != ka {
				t.Fatalf("NormalizeImportName is not idempotent for %q: got %q", ka, NormalizeImportName(ka))
			}
		})
	}

	for _, blank := range []string{"", "   ", "\t\n\u00a0\u3000"} {
		if got := NormalizeImportName(blank); got != "" {
			t.Errorf("NormalizeImportName(%q) = %q, want empty string", blank, got)
		}
	}
}

func TestPersonValidate(t *testing.T) {
	key := "eiichiro oda"
	emptyKey := "   "

	cases := []struct {
		name    string
		person  Person
		wantErr error
	}{
		{
			name:   "valid curated without import key",
			person: Person{ID: "p1", Name: "Eiichiro Oda"},
		},
		{
			name:   "valid with import key",
			person: Person{ID: "p1", Name: "Eiichiro Oda", ImportKey: &key},
		},
		{
			name:    "empty id",
			person:  Person{ID: " ", Name: "Eiichiro Oda"},
			wantErr: ErrEmptyPersonID,
		},
		{
			name:    "empty name",
			person:  Person{ID: "p1", Name: "\u3000 "},
			wantErr: ErrEmptyPersonName,
		},
		{
			name:    "empty import key when non-nil",
			person:  Person{ID: "p1", Name: "Eiichiro Oda", ImportKey: &emptyKey},
			wantErr: ErrEmptyImportKey,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.person.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Person.Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestCreditValidate(t *testing.T) {
	sid := "s1"
	vid := "v1"
	empty := "  "

	cases := []struct {
		name    string
		credit  Credit
		wantErr error
	}{
		{
			name:   "valid series credit",
			credit: Credit{ID: "c1", PersonID: "p1", SeriesID: &sid, Role: CreditRoleWriter},
		},
		{
			name:   "valid volume credit",
			credit: Credit{ID: "c1", PersonID: "p1", VolumeID: &vid, Role: CreditRoleTranslator},
		},
		{
			name:    "empty credit id",
			credit:  Credit{ID: "", PersonID: "p1", VolumeID: &vid, Role: CreditRoleWriter},
			wantErr: ErrEmptyCreditID,
		},
		{
			name:    "empty person id",
			credit:  Credit{ID: "c1", PersonID: " ", VolumeID: &vid, Role: CreditRoleWriter},
			wantErr: ErrEmptyPersonID,
		},
		{
			name:    "neither series_id nor volume_id",
			credit:  Credit{ID: "c1", PersonID: "p1", Role: CreditRoleWriter},
			wantErr: ErrInvalidCreditTarget,
		},
		{
			name:    "both series_id and volume_id",
			credit:  Credit{ID: "c1", PersonID: "p1", SeriesID: &sid, VolumeID: &vid, Role: CreditRoleWriter},
			wantErr: ErrInvalidCreditTarget,
		},
		{
			name:    "empty series_id pointer",
			credit:  Credit{ID: "c1", PersonID: "p1", SeriesID: &empty, Role: CreditRoleWriter},
			wantErr: ErrEmptySeriesID,
		},
		{
			name:    "empty volume_id pointer",
			credit:  Credit{ID: "c1", PersonID: "p1", VolumeID: &empty, Role: CreditRoleWriter},
			wantErr: ErrEmptyVolumeID,
		},
		{
			name:    "invalid role",
			credit:  Credit{ID: "c1", PersonID: "p1", SeriesID: &sid, Role: CreditRole("INVALID")},
			wantErr: ErrInvalidCreditRole,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.credit.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Credit.Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestValidateCreditsForTarget(t *testing.T) {
	sid := "s1"
	otherSID := "s2"
	vid := "v1"

	if err := ValidateSeriesCredits(sid, []Credit{
		{ID: "c1", PersonID: "p1", SeriesID: &sid, Role: CreditRoleWriter, Position: 0},
		{ID: "c2", PersonID: "p1", SeriesID: &sid, Role: CreditRoleArtist, Position: 1},
	}); err != nil {
		t.Fatalf("ValidateSeriesCredits valid failed: %v", err)
	}

	if err := ValidateSeriesCredits(sid, []Credit{
		{ID: "c1", PersonID: "p1", SeriesID: &sid, Role: CreditRoleWriter},
		{ID: "c2", PersonID: "p1", SeriesID: &sid, Role: CreditRoleWriter},
	}); !errors.Is(err, ErrDuplicateCredit) {
		t.Fatalf("ValidateSeriesCredits duplicate = %v, want ErrDuplicateCredit", err)
	}

	if err := ValidateSeriesCredits(sid, []Credit{
		{ID: "c1", PersonID: "p1", SeriesID: &otherSID, Role: CreditRoleWriter},
	}); err == nil {
		t.Fatal("expected target mismatch error for wrong series ID")
	}

	if err := ValidateVolumeCredits(vid, []Credit{
		{ID: "c1", PersonID: "p1", VolumeID: &vid, Role: CreditRoleWriter, Position: 0},
		{ID: "c2", PersonID: "p2", VolumeID: &vid, Role: CreditRolePenciller, Position: 1},
	}); err != nil {
		t.Fatalf("ValidateVolumeCredits valid failed: %v", err)
	}
}

func TestEntityAndVolumeEntityValidate(t *testing.T) {
	key := "luffy"
	emptyKey := ""

	cases := []struct {
		name    string
		entity  Entity
		wantErr error
	}{
		{
			name:   "valid curated entity",
			entity: Entity{ID: "e1", Kind: EntityKindCharacter, Name: "Luffy"},
		},
		{
			name:   "valid with import key",
			entity: Entity{ID: "e1", Kind: EntityKindLocation, Name: "Gotham", ImportKey: &key},
		},
		{
			name:    "empty id",
			entity:  Entity{ID: "", Kind: EntityKindTeam, Name: "Avengers"},
			wantErr: ErrEmptyEntityID,
		},
		{
			name:    "invalid kind",
			entity:  Entity{ID: "e1", Kind: EntityKind("ITEM"), Name: "X"},
			wantErr: ErrInvalidEntityKind,
		},
		{
			name:    "empty name",
			entity:  Entity{ID: "e1", Kind: EntityKindTeam, Name: "  "},
			wantErr: ErrEmptyEntityName,
		},
		{
			name:    "empty import key",
			entity:  Entity{ID: "e1", Kind: EntityKindCharacter, Name: "Luffy", ImportKey: &emptyKey},
			wantErr: ErrEmptyImportKey,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.entity.Validate()
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Entity.Validate() = %v, want %v", err, tc.wantErr)
			}
		})
	}

	if err := ValidateVolumeEntities("v1", []VolumeEntity{
		{VolumeID: "v1", EntityID: "e1"},
		{VolumeID: "v1", EntityID: "e2"},
	}); err != nil {
		t.Fatalf("ValidateVolumeEntities valid failed: %v", err)
	}

	if err := ValidateVolumeEntities("v1", []VolumeEntity{
		{VolumeID: "v1", EntityID: "e1"},
		{VolumeID: "v1", EntityID: "e1"},
	}); !errors.Is(err, ErrDuplicateVolumeEntity) {
		t.Fatalf("ValidateVolumeEntities duplicate = %v, want ErrDuplicateVolumeEntity", err)
	}

	if err := ValidateVolumeEntities("v1", []VolumeEntity{
		{VolumeID: "v2", EntityID: "e1"},
	}); err == nil {
		t.Fatal("expected volume mismatch error")
	}
}
