package postgres_test

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestLabelPredicate_FailClosedAndParameterization(t *testing.T) {
	readerRole, ok := domain.BuiltInRoleByName(domain.RoleReaderName)
	if !ok {
		t.Fatal("Reader role missing")
	}
	restrictedRole, ok := domain.BuiltInRoleByName(domain.RoleRestrictedReaderName)
	if !ok {
		t.Fatal("Restricted reader role missing")
	}
	adminRole, ok := domain.BuiltInRoleByName(domain.RoleAdminName)
	if !ok {
		t.Fatal("Admin role missing")
	}

	user := domain.User{ID: "u-1", Username: "alice"}
	adminPrincipal := domain.NewPrincipal(user, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-admin", nil)
	adminReadPAT := domain.NewPrincipal(user, []domain.Role{adminRole}, domain.CredentialKindPAT, "c-pat", []domain.PATScope{domain.ScopeRead})
	restrictedPrincipal := domain.NewPrincipal(user, []domain.Role{restrictedRole}, domain.CredentialKindPassword, "c-rest", nil)
	multiRolePrincipal := domain.NewPrincipal(user, []domain.Role{readerRole, restrictedRole}, domain.CredentialKindPassword, "c-multi", nil)

	t.Run("nil receiver fails closed", func(t *testing.T) {
		var nilPred *postgres.LabelPredicate
		if !nilPred.IsDenyAll() {
			t.Fatal("nil predicate must report IsDenyAll=true")
		}
		if nilPred.IncludesTrashed() {
			t.Fatal("nil predicate must not include trashed")
		}
		for _, fn := range []func(string, int) (postgres.SQLFragment, error){
			nilPred.SeriesFragment,
			nilPred.VolumeFragment,
			nilPred.AssetFragment,
			nilPred.ReadingProgressFragment,
		} {
			frag, err := fn("t", 1)
			if err != nil {
				t.Fatalf("unexpected error for nil predicate: %v", err)
			}
			if frag.SQL != "FALSE" || len(frag.Args) != 0 {
				t.Fatalf("expected FALSE with 0 args, got %+v", frag)
			}
		}
	})

	t.Run("zero-value predicate fails closed", func(t *testing.T) {
		for _, zeroPred := range []*postgres.LabelPredicate{{}, new(postgres.LabelPredicate)} {
			if !zeroPred.IsDenyAll() {
				t.Fatal("zero-value predicate must report IsDenyAll=true")
			}
			if zeroPred.IncludesTrashed() {
				t.Fatal("zero-value predicate must not include trashed")
			}
			if zeroPred.AllowIDs() != nil || zeroPred.RestrictIDs() != nil {
				t.Fatalf("zero-value predicate must return nil AllowIDs/RestrictIDs, got allow=%v restrict=%v",
					zeroPred.AllowIDs(), zeroPred.RestrictIDs())
			}
			for _, fn := range []func(string, int) (postgres.SQLFragment, error){
				zeroPred.SeriesFragment,
				zeroPred.VolumeFragment,
				zeroPred.AssetFragment,
				zeroPred.ReadingProgressFragment,
			} {
				frag, err := fn("t", 1)
				if err != nil {
					t.Fatalf("unexpected error for zero-value predicate: %v", err)
				}
				if frag.SQL != "FALSE" || len(frag.Args) != 0 {
					t.Fatalf("expected FALSE with 0 args, got %+v", frag)
				}
			}
		}
	})

	t.Run("missing or empty-user principal fails closed even with IncludeTrashed", func(t *testing.T) {
		for _, p := range []*domain.Principal{
			nil,
			domain.NewPrincipal(domain.User{ID: "", Username: "anon"}, []domain.Role{adminRole}, domain.CredentialKindPassword, "c1", nil),
		} {
			pred := postgres.NewLabelPredicate(p, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
			if !pred.IsDenyAll() {
				t.Fatalf("expected IsDenyAll for principal %+v", p)
			}
			frag, err := pred.SeriesFragment("s", 1)
			if err != nil || frag.SQL != "FALSE" || len(frag.Args) != 0 {
				t.Fatalf("expected FALSE with no args, got frag=%+v err=%v", frag, err)
			}
		}
	})

	t.Run("Restricted reader and multi-role fail closed with zero policies or cancelled allow", func(t *testing.T) {
		pZero := postgres.NewLabelPredicate(restrictedPrincipal, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		if !pZero.IsDenyAll() {
			t.Fatal("Restricted reader with 0 policies must fail closed")
		}

		pMulti := postgres.NewLabelPredicate(multiRolePrincipal, nil, nil, postgres.LabelPredicateOptions{})
		if !pMulti.IsDenyAll() {
			t.Fatal("multi-role with requires_allow_labels and 0 policies must fail closed")
		}

		pCancelled := postgres.NewLabelPredicate(
			restrictedPrincipal,
			[]domain.UserLabelPolicy{{UserID: user.ID, LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow}},
			[]domain.GroupLabelPolicy{{GroupClaim: "kids", LabelID: "lbl-kids", Effect: domain.PolicyEffectRestrict}},
			postgres.LabelPredicateOptions{},
		)
		if !pCancelled.IsDenyAll() {
			t.Fatal("Restricted reader whose only ALLOW is cancelled by group RESTRICT must fail closed")
		}
	})

	t.Run("trash filter follows effective permissions not role name", func(t *testing.T) {
		pAdminNormal := postgres.NewLabelPredicate(adminPrincipal, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: false})
		if pAdminNormal.IncludesTrashed() {
			t.Fatal("IncludeTrashed=false must still exclude trashed items")
		}
		if !slices.Contains(pAdminNormal.RestrictIDs(), domain.LabelMarkedForDeletionID) {
			t.Fatalf("expected marked_for_deletion in RestrictIDs, got %v", pAdminNormal.RestrictIDs())
		}

		pAdminTrashView := postgres.NewLabelPredicate(adminPrincipal, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		if !pAdminTrashView.IncludesTrashed() {
			t.Fatal("Admin with PermissionTrashPurge and IncludeTrashed=true should include trashed items")
		}
		frag, err := pAdminTrashView.SeriesFragment("s", 1)
		if err != nil || frag.SQL != "TRUE" || len(frag.Args) != 0 {
			t.Fatalf("expected unconstrained admin trash view fragment TRUE, got %+v err=%v", frag, err)
		}

		pReadPAT := postgres.NewLabelPredicate(adminReadPAT, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		if pReadPAT.IncludesTrashed() {
			t.Fatal("Admin with read-only PAT lacks PermissionTrashPurge and must filter trash")
		}
		if !slices.Contains(pReadPAT.RestrictIDs(), domain.LabelMarkedForDeletionID) {
			t.Fatalf("expected marked_for_deletion in read-PAT RestrictIDs, got %v", pReadPAT.RestrictIDs())
		}
	})

	t.Run("label IDs are strictly parameterized and never interpolated into SQL", func(t *testing.T) {
		sentinelAllow := "lbl_allow_' OR '1'='1"
		sentinelRestrict := "lbl_restrict_; DROP TABLE series;--"

		p := domain.NewPrincipal(user, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-1", nil)
		pred := postgres.NewLabelPredicate(
			p,
			[]domain.UserLabelPolicy{
				{UserID: user.ID, LabelID: sentinelAllow, Effect: domain.PolicyEffectAllow},
				{UserID: user.ID, LabelID: sentinelRestrict, Effect: domain.PolicyEffectRestrict},
			},
			nil,
			postgres.LabelPredicateOptions{},
		)

		for _, fn := range []func(string, int) (postgres.SQLFragment, error){
			pred.SeriesFragment,
			pred.VolumeFragment,
			pred.AssetFragment,
			pred.ReadingProgressFragment,
		} {
			frag, err := fn("tbl", 3)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if strings.Contains(frag.SQL, sentinelAllow) || strings.Contains(frag.SQL, sentinelRestrict) || strings.Contains(frag.SQL, domain.LabelMarkedForDeletionID) {
				t.Fatalf("SQL fragment leaked literal label ID: %s", frag.SQL)
			}
			if !strings.Contains(frag.SQL, "$3::text[]") || !strings.Contains(frag.SQL, "$4::text[]") {
				t.Fatalf("expected parameterized $3::text[] and $4::text[], got: %s", frag.SQL)
			}
			if len(frag.Args) != 2 {
				t.Fatalf("expected 2 args, got %d", len(frag.Args))
			}
			allowArg, ok := frag.Args[0].([]string)
			if !ok || !slices.Equal(allowArg, []string{sentinelAllow}) {
				t.Fatalf("unexpected allow arg: %#v", frag.Args[0])
			}
			restrictArg, ok := frag.Args[1].([]string)
			if !ok || !slices.Contains(restrictArg, sentinelRestrict) || !slices.Contains(restrictArg, domain.LabelMarkedForDeletionID) {
				t.Fatalf("unexpected restrict arg: %#v", frag.Args[1])
			}
		}
	})

	t.Run("defensive clones, empty alias defaults, and invalid alias/startParam rejection", func(t *testing.T) {
		p := domain.NewPrincipal(user, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-1", nil)
		pred := postgres.NewLabelPredicate(
			p,
			[]domain.UserLabelPolicy{{UserID: user.ID, LabelID: "lbl-1", Effect: domain.PolicyEffectAllow}},
			nil,
			postgres.LabelPredicateOptions{},
		)

		allows := pred.AllowIDs()
		restricts := pred.RestrictIDs()
		allows[0] = "mutated"
		restricts[0] = "mutated"
		if pred.AllowIDs()[0] != "lbl-1" || pred.RestrictIDs()[0] != domain.LabelMarkedForDeletionID {
			t.Fatal("AllowIDs/RestrictIDs must return defensive clones")
		}

		// Empty alias defaults to canonical table names.
		sFrag, err := pred.SeriesFragment("", 1)
		if err != nil || !strings.Contains(sFrag.SQL, "series.id") {
			t.Fatalf("expected series.id default qualifier, got %+v err=%v", sFrag, err)
		}
		vFrag, err := pred.VolumeFragment("", 1)
		if err != nil || !strings.Contains(vFrag.SQL, "volumes.id") {
			t.Fatalf("expected volumes.id default qualifier, got %+v err=%v", vFrag, err)
		}
		aFrag, err := pred.AssetFragment("", 1)
		if err != nil || !strings.Contains(aFrag.SQL, "media_assets.id") || !strings.Contains(aFrag.SQL, "media_assets.volume_id") {
			t.Fatalf("expected media_assets qualifiers, got %+v err=%v", aFrag, err)
		}
		rpFrag, err := pred.ReadingProgressFragment("", 1)
		if err != nil || !strings.Contains(rpFrag.SQL, "reading_progress.volume_id") {
			t.Fatalf("expected reading_progress.volume_id qualifier, got %+v err=%v", rpFrag, err)
		}

		// Invalid alias or startParam fails closed with FALSE and error.
		for _, badAlias := range []string{"s; DROP TABLE series", "s.id", "1bad", "a b"} {
			frag, err := pred.SeriesFragment(badAlias, 1)
			if err == nil || frag.SQL != "FALSE" {
				t.Fatalf("expected error + FALSE for alias %q, got frag=%+v err=%v", badAlias, frag, err)
			}
		}
		frag, err := pred.VolumeFragment("v", 0)
		if err == nil || frag.SQL != "FALSE" {
			t.Fatalf("expected error + FALSE for startParam=0, got frag=%+v err=%v", frag, err)
		}
	})

	t.Run("ALLOW marked_for_deletion with IncludeTrashed=false does not widen to allow-all", func(t *testing.T) {
		p := domain.NewPrincipal(user, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-1", nil)
		pred := postgres.NewLabelPredicate(
			p,
			[]domain.UserLabelPolicy{{UserID: user.ID, LabelID: domain.LabelMarkedForDeletionID, Effect: domain.PolicyEffectAllow}},
			nil,
			postgres.LabelPredicateOptions{IncludeTrashed: false},
		)
		if len(pred.AllowIDs()) != 1 || pred.AllowIDs()[0] != domain.LabelMarkedForDeletionID {
			t.Fatalf("expected AllowIDs to retain marked_for_deletion so unlabeled items stay hidden, got %v", pred.AllowIDs())
		}
		if !slices.Contains(pred.RestrictIDs(), domain.LabelMarkedForDeletionID) {
			t.Fatalf("expected RestrictIDs to include marked_for_deletion, got %v", pred.RestrictIDs())
		}
	})

	t.Run("user policy rows for other or empty users are ignored", func(t *testing.T) {
		pRest := postgres.NewLabelPredicate(
			restrictedPrincipal,
			[]domain.UserLabelPolicy{
				{UserID: "other-user", LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow},
				{UserID: "", LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow},
			},
			nil,
			postgres.LabelPredicateOptions{},
		)
		if !pRest.IsDenyAll() {
			t.Fatal("Restricted reader must fail closed when only foreign/empty-user ALLOW rows are supplied")
		}

		pMixed := postgres.NewLabelPredicate(
			restrictedPrincipal,
			[]domain.UserLabelPolicy{
				{UserID: user.ID, LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow},
				{UserID: "other-user", LabelID: "lbl-nsfw", Effect: domain.PolicyEffectAllow},
				{UserID: "other-user", LabelID: "lbl-kids", Effect: domain.PolicyEffectRestrict},
				{UserID: "", LabelID: "lbl-extra", Effect: domain.PolicyEffectAllow},
			},
			nil,
			postgres.LabelPredicateOptions{},
		)
		if pMixed.IsDenyAll() {
			t.Fatal("foreign RESTRICT must not cancel principal's own ALLOW")
		}
		if !slices.Equal(pMixed.AllowIDs(), []string{"lbl-kids"}) {
			t.Fatalf("expected only principal's ALLOW lbl-kids, got %v", pMixed.AllowIDs())
		}
		if !slices.Equal(pMixed.RestrictIDs(), []string{domain.LabelMarkedForDeletionID}) {
			t.Fatalf("expected only marked_for_deletion restrict, got %v", pMixed.RestrictIDs())
		}

		pReader := domain.NewPrincipal(user, []domain.Role{readerRole}, domain.CredentialKindPassword, "c-reader", nil)
		pReaderPred := postgres.NewLabelPredicate(
			pReader,
			[]domain.UserLabelPolicy{
				{UserID: "other-user", LabelID: "lbl-kids", Effect: domain.PolicyEffectAllow},
				{UserID: "other-user", LabelID: "lbl-nsfw", Effect: domain.PolicyEffectRestrict},
			},
			nil,
			postgres.LabelPredicateOptions{},
		)
		if len(pReaderPred.AllowIDs()) != 0 {
			t.Fatalf("foreign ALLOW rows must not narrow principal; got %v", pReaderPred.AllowIDs())
		}
		if !slices.Equal(pReaderPred.RestrictIDs(), []string{domain.LabelMarkedForDeletionID}) {
			t.Fatalf("foreign RESTRICT rows must not apply to principal; got %v", pReaderPred.RestrictIDs())
		}
	})

	t.Run("LoadLabelPredicate and LabelRepository.LabelPredicate fail closed on error", func(t *testing.T) {
		ctx := context.Background()
		p := domain.NewPrincipal(user, []domain.Role{adminRole}, domain.CredentialKindPassword, "c-1", nil)

		predNilDB, err := postgres.LoadLabelPredicate(ctx, nil, p, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		if err == nil || !predNilDB.IsDenyAll() {
			t.Fatalf("expected error and IsDenyAll on nil db, got pred=%+v err=%v", predNilDB, err)
		}

		predNilPrincipal, err := postgres.LoadLabelPredicate(ctx, nil, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		if err != nil || !predNilPrincipal.IsDenyAll() {
			t.Fatalf("expected nil error and IsDenyAll on nil principal, got pred=%+v err=%v", predNilPrincipal, err)
		}

		var nilRepo *postgres.LabelRepository
		predNilRepo, err := nilRepo.LabelPredicate(ctx, p, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		if err == nil || !predNilRepo.IsDenyAll() {
			t.Fatalf("expected error and IsDenyAll on nil repo, got pred=%+v err=%v", predNilRepo, err)
		}
	})
}

type seededCatalogue struct {
	tdb       *testdb.TestDB
	userRepo  *postgres.UserRepository
	labelRepo *postgres.LabelRepository

	libraryID string
	rootID    string

	// Users
	adminUser      *domain.User
	readerUser     *domain.User
	restrictedUser *domain.User
	multiRoleUser  *domain.User

	// Roles
	adminRole      domain.Role
	readerRole     domain.Role
	restrictedRole domain.Role

	// SERVER labels
	lblKids    string
	lblAllAges string
	lblNSFW    string
	lblGore    string
	lblTrash   string

	// Series IDs
	sClean        string
	sKids         string
	sAllAges      string
	sNSFW         string
	sKidsNSFW     string
	sTrashed      string
	sUserKidsOnly string

	// Volume IDs
	vInClean                  string
	vInKids                   string
	vAllowedVolInClean        string
	vInKidsVolRestricted      string
	vInKidsAndClean           string
	vAltPathRestrictedPrimary string
	vAltPathRestrictedSec     string
	vInTrashedSeries          string
	vTrashedInKids            string
	vStandaloneUnlabeled      string
	vStandaloneKids           string
	vStandaloneNSFW           string
	vStandaloneKidsNSFW       string
	vStandaloneTrashed        string
	vZeroAssetsInKids         string

	// Asset IDs
	aKidsClean                  string
	aKidsNSFW                   string
	aKidsTrashed                string
	aAllowedInHiddenVol         string
	aCleanInRestrictedVol       string
	aKidsInRestrictedVol        string
	aInAltRestricted            string
	aAllowedInUnallowedStandal  string
	aStandaloneClean            string
	aStandaloneNSFW             string
	aInClean                    string
	aInTrashedSeries            string
	aInTrashedVol               string
	aInKidsAndClean             string
	aInAllAges                  string
	vInAllAges                  string
	aAltPrimaryRestricted       string
	aStandaloneUnlabeledDefault string

	// Domain evaluation fixtures mirroring seeded catalogue
	seriesLabels map[string][]string
	volumeLabels map[string][]string
	volumeSeries map[string][][]string
	assetLabels  map[string][]string
	assetVolume  map[string]string

	allSeriesIDs []string
	allVolumeIDs []string
	allAssetIDs  []string
	progressVols []string
	progressUser string
}

func setupSeededCatalogue(t *testing.T) *seededCatalogue {
	t.Helper()
	ctx := context.Background()
	tdb := testdb.New(t)
	if err := migrations.RunFS(ctx, tdb.Pool, migrations.CoreFS); err != nil {
		t.Fatalf("RunFS(CoreFS): %v", err)
	}

	userRepo := postgres.NewUserRepository(tdb.Pool)
	labelRepo := postgres.NewLabelRepository(tdb.Pool)

	c := &seededCatalogue{
		tdb:          tdb,
		userRepo:     userRepo,
		labelRepo:    labelRepo,
		lblTrash:     domain.LabelMarkedForDeletionID,
		seriesLabels: make(map[string][]string),
		volumeLabels: make(map[string][]string),
		volumeSeries: make(map[string][][]string),
		assetLabels:  make(map[string][]string),
		assetVolume:  make(map[string]string),
	}

	var ok bool
	if c.adminRole, ok = domain.BuiltInRoleByName(domain.RoleAdminName); !ok {
		t.Fatal("Admin role not found")
	}
	if c.readerRole, ok = domain.BuiltInRoleByName(domain.RoleReaderName); !ok {
		t.Fatal("Reader role not found")
	}
	if c.restrictedRole, ok = domain.BuiltInRoleByName(domain.RoleRestrictedReaderName); !ok {
		t.Fatal("Restricted reader role not found")
	}

	c.adminUser = createSeededTestUser(ctx, t, userRepo, "admin")
	c.readerUser = createSeededTestUser(ctx, t, userRepo, "reader")
	c.restrictedUser = createSeededTestUser(ctx, t, userRepo, "timmy")
	c.multiRoleUser = createSeededTestUser(ctx, t, userRepo, "multi")

	for _, grant := range []struct {
		userID string
		roleID string
	}{
		{c.adminUser.ID, c.adminRole.ID},
		{c.readerUser.ID, c.readerRole.ID},
		{c.restrictedUser.ID, c.restrictedRole.ID},
		{c.multiRoleUser.ID, c.readerRole.ID},
		{c.multiRoleUser.ID, c.restrictedRole.ID},
	} {
		if err := userRepo.AssignUserRole(ctx, grant.userID, grant.roleID); err != nil {
			t.Fatalf("AssignUserRole: %v", err)
		}
	}

	// Create SERVER labels.
	c.lblKids = c.mustCreateServerLabel(ctx, t, "kids")
	c.lblAllAges = c.mustCreateServerLabel(ctx, t, "all-ages")
	c.lblNSFW = c.mustCreateServerLabel(ctx, t, "nsfw")
	c.lblGore = c.mustCreateServerLabel(ctx, t, "gore")

	// Create custom USER labels colliding in name with SERVER labels to prove USER scope isolation.
	readerUserKids := c.mustCreateUserLabel(ctx, t, c.readerUser.ID, "kids")
	readerUserNSFW := c.mustCreateUserLabel(ctx, t, c.readerUser.ID, "nsfw")
	restrictedUserKids := c.mustCreateUserLabel(ctx, t, c.restrictedUser.ID, "kids")

	readerFav, err := labelRepo.GetUserLabelByName(ctx, c.readerUser.ID, domain.LabelFavorite)
	if err != nil {
		t.Fatalf("GetUserLabelByName(favorite): %v", err)
	}

	// Create library and root.
	c.libraryID = domain.MustNewID()
	c.rootID = domain.MustNewID()
	if _, err := tdb.Exec(ctx, `INSERT INTO libraries (id, name) VALUES ($1, $2)`, c.libraryID, "Main Library"); err != nil {
		t.Fatalf("insert library: %v", err)
	}
	if _, err := tdb.Exec(ctx, `INSERT INTO library_roots (id, library_id, path_bytes, path_display, path_match) VALUES ($1, $2, $3, $4, $4)`,
		c.rootID, c.libraryID, []byte("/manga"), "/manga"); err != nil {
		t.Fatalf("insert root: %v", err)
	}

	// Series
	c.sClean = c.mustCreateSeries(ctx, t, "Clean Series")
	c.sKids = c.mustCreateSeries(ctx, t, "Kids Series", c.lblKids)
	c.sAllAges = c.mustCreateSeries(ctx, t, "All Ages Series", c.lblAllAges)
	c.sNSFW = c.mustCreateSeries(ctx, t, "NSFW Series", c.lblNSFW)
	c.sKidsNSFW = c.mustCreateSeries(ctx, t, "Kids+NSFW Series", c.lblKids, c.lblNSFW)
	c.sTrashed = c.mustCreateSeries(ctx, t, "Trashed Series", c.lblKids, c.lblTrash)
	c.sUserKidsOnly = c.mustCreateSeries(ctx, t, "User-Labeled Series")

	// Attach USER labels to series (must never affect visibility).
	c.mustAssignRawLabel(ctx, t, readerUserNSFW, domain.ItemTypeSeries, c.sClean)
	c.mustAssignRawLabel(ctx, t, readerUserNSFW, domain.ItemTypeSeries, c.sKids)
	c.mustAssignRawLabel(ctx, t, readerFav.ID, domain.ItemTypeSeries, c.sClean)
	c.mustAssignRawLabel(ctx, t, readerUserKids, domain.ItemTypeSeries, c.sUserKidsOnly)
	c.mustAssignRawLabel(ctx, t, restrictedUserKids, domain.ItemTypeSeries, c.sUserKidsOnly)

	// Volumes with series memberships
	c.vInClean = c.mustCreateVolume(ctx, t, "V in Clean", []string{c.sClean})
	c.vInKids = c.mustCreateVolume(ctx, t, "V in Kids", []string{c.sKids})
	c.vInAllAges = c.mustCreateVolume(ctx, t, "V in All Ages", []string{c.sAllAges})
	c.vAllowedVolInClean = c.mustCreateVolume(ctx, t, "Allowed Vol in Clean", []string{c.sClean}, c.lblKids)
	c.vInKidsVolRestricted = c.mustCreateVolume(ctx, t, "Restricted Vol in Kids", []string{c.sKids}, c.lblNSFW)
	c.vInKidsAndClean = c.mustCreateVolume(ctx, t, "V in Kids (primary) + Clean (secondary)", []string{c.sKids, c.sClean})
	c.vAltPathRestrictedPrimary = c.mustCreateVolume(ctx, t, "V in NSFW (primary) + Kids (secondary)", []string{c.sNSFW, c.sKids})
	c.vAltPathRestrictedSec = c.mustCreateVolume(ctx, t, "V in Kids (primary) + NSFW (secondary)", []string{c.sKids, c.sNSFW})
	c.vInTrashedSeries = c.mustCreateVolume(ctx, t, "V in Trashed Series", []string{c.sTrashed})
	c.vTrashedInKids = c.mustCreateVolume(ctx, t, "Trashed Vol in Kids", []string{c.sKids}, c.lblTrash)
	c.vZeroAssetsInKids = c.mustCreateVolume(ctx, t, "Zero-Asset Vol in Kids", []string{c.sKids})

	// Standalone volumes (no memberships)
	c.vStandaloneUnlabeled = c.mustCreateVolume(ctx, t, "Standalone Unlabeled", nil)
	c.vStandaloneKids = c.mustCreateVolume(ctx, t, "Standalone Kids", nil, c.lblKids)
	c.vStandaloneNSFW = c.mustCreateVolume(ctx, t, "Standalone NSFW", nil, c.lblNSFW)
	c.vStandaloneKidsNSFW = c.mustCreateVolume(ctx, t, "Standalone Kids+NSFW", nil, c.lblKids, c.lblNSFW)
	c.vStandaloneTrashed = c.mustCreateVolume(ctx, t, "Standalone Trashed", nil, c.lblKids, c.lblTrash)

	// Attach USER labels to standalone & member volumes (must never grant or restrict).
	c.mustAssignRawLabel(ctx, t, restrictedUserKids, domain.ItemTypeVolume, c.vStandaloneUnlabeled)
	c.mustAssignRawLabel(ctx, t, readerUserKids, domain.ItemTypeVolume, c.vStandaloneUnlabeled)
	c.mustAssignRawLabel(ctx, t, readerUserNSFW, domain.ItemTypeVolume, c.vInKids)

	// Assets
	c.aKidsClean = c.mustCreateAsset(ctx, t, c.vInKids, "kids-clean.cbz")
	c.aKidsNSFW = c.mustCreateAsset(ctx, t, c.vInKids, "kids-nsfw.cbz", c.lblNSFW)
	c.aKidsTrashed = c.mustCreateAsset(ctx, t, c.vInKids, "kids-trashed.cbz", c.lblTrash)
	c.aAllowedInHiddenVol = c.mustCreateAsset(ctx, t, c.vAllowedVolInClean, "allowed-in-hidden-vol.cbz", c.lblKids)
	c.aCleanInRestrictedVol = c.mustCreateAsset(ctx, t, c.vInKidsVolRestricted, "clean-in-restricted-vol.cbz")
	c.aKidsInRestrictedVol = c.mustCreateAsset(ctx, t, c.vInKidsVolRestricted, "kids-in-restricted-vol.cbz", c.lblKids)
	c.aInAltRestricted = c.mustCreateAsset(ctx, t, c.vAltPathRestrictedSec, "in-alt-restricted.cbz")
	c.aAltPrimaryRestricted = c.mustCreateAsset(ctx, t, c.vAltPathRestrictedPrimary, "in-alt-primary-restricted.cbz")
	c.aAllowedInUnallowedStandal = c.mustCreateAsset(ctx, t, c.vStandaloneUnlabeled, "allowed-in-unallowed-standalone.cbz", c.lblKids)
	c.aStandaloneUnlabeledDefault = c.mustCreateAsset(ctx, t, c.vStandaloneUnlabeled, "unallowed-standalone.cbz")
	c.aStandaloneClean = c.mustCreateAsset(ctx, t, c.vStandaloneKids, "standalone-clean.cbz")
	c.aStandaloneNSFW = c.mustCreateAsset(ctx, t, c.vStandaloneKids, "standalone-nsfw.cbz", c.lblNSFW)
	c.aInClean = c.mustCreateAsset(ctx, t, c.vInClean, "in-clean.cbz")
	c.aInTrashedSeries = c.mustCreateAsset(ctx, t, c.vInTrashedSeries, "in-trashed-series.cbz")
	c.aInTrashedVol = c.mustCreateAsset(ctx, t, c.vTrashedInKids, "in-trashed-vol.cbz")
	c.aInKidsAndClean = c.mustCreateAsset(ctx, t, c.vInKidsAndClean, "in-kids-and-clean.cbz")
	c.aInAllAges = c.mustCreateAsset(ctx, t, c.vInAllAges, "in-all-ages.cbz")

	// Attach USER label to an asset (must never restrict or grant).
	c.mustAssignRawLabel(ctx, t, readerUserNSFW, domain.ItemTypeAsset, c.aKidsClean)
	c.mustAssignRawLabel(ctx, t, restrictedUserKids, domain.ItemTypeAsset, c.aStandaloneUnlabeledDefault)

	// Reading progress for readerUser across visible, restricted, and trashed volumes.
	c.progressUser = c.readerUser.ID
	c.progressVols = []string{
		c.vInKids,
		c.vInKidsVolRestricted,
		c.vAltPathRestrictedSec,
		c.vAllowedVolInClean,
		c.vTrashedInKids,
		c.vStandaloneKids,
	}
	for _, vid := range c.progressVols {
		if _, err := tdb.Exec(ctx, `
			INSERT INTO reading_progress (user_id, volume_id, locator, total_progression, source, completed)
			VALUES ($1, $2, '{"type":"page","page":1}', 0.1, 'REST', FALSE)`,
			c.progressUser, vid,
		); err != nil {
			t.Fatalf("insert reading_progress for %s: %v", vid, err)
		}
	}

	sort.Strings(c.allSeriesIDs)
	sort.Strings(c.allVolumeIDs)
	sort.Strings(c.allAssetIDs)

	return c
}

func (c *seededCatalogue) mustCreateServerLabel(ctx context.Context, t *testing.T, name string) string {
	t.Helper()
	id := domain.MustNewID()
	if err := c.labelRepo.CreateLabel(ctx, &domain.Label{
		ID:    id,
		Scope: domain.LabelScopeServer,
		Name:  name,
	}); err != nil {
		t.Fatalf("CreateLabel(%s): %v", name, err)
	}
	return id
}

func (c *seededCatalogue) mustCreateUserLabel(ctx context.Context, t *testing.T, ownerID, name string) string {
	t.Helper()
	id := domain.MustNewID()
	if err := c.labelRepo.CreateLabel(ctx, &domain.Label{
		ID:      id,
		Scope:   domain.LabelScopeUser,
		OwnerID: &ownerID,
		Name:    name,
	}); err != nil {
		t.Fatalf("CreateLabel(user=%s,%s): %v", ownerID, name, err)
	}
	return id
}

func (c *seededCatalogue) mustAssignRawLabel(ctx context.Context, t *testing.T, labelID string, itemType domain.ItemType, itemID string) {
	t.Helper()
	if err := c.labelRepo.AssignItemLabel(ctx, &domain.ItemLabel{
		LabelID:  labelID,
		ItemType: itemType,
		ItemID:   itemID,
	}); err != nil {
		t.Fatalf("AssignItemLabel(%s, %s, %s): %v", labelID, itemType, itemID, err)
	}
}

func (c *seededCatalogue) mustCreateSeries(ctx context.Context, t *testing.T, title string, serverLabelIDs ...string) string {
	t.Helper()
	id := domain.MustNewID()
	if _, err := c.tdb.Exec(ctx, `
		INSERT INTO series (id, library_id, title, title_sort)
		VALUES ($1, $2, $3, $3)`,
		id, c.libraryID, title,
	); err != nil {
		t.Fatalf("insert series %q: %v", title, err)
	}
	for _, lid := range serverLabelIDs {
		c.mustAssignRawLabel(ctx, t, lid, domain.ItemTypeSeries, id)
	}
	c.seriesLabels[id] = slices.Clone(serverLabelIDs)
	c.allSeriesIDs = append(c.allSeriesIDs, id)
	return id
}

func (c *seededCatalogue) mustCreateVolume(ctx context.Context, t *testing.T, title string, seriesIDs []string, serverLabelIDs ...string) string {
	t.Helper()
	id := domain.MustNewID()
	if _, err := c.tdb.Exec(ctx, `
		INSERT INTO volumes (id, library_id, title)
		VALUES ($1, $2, $3)`,
		id, c.libraryID, title,
	); err != nil {
		t.Fatalf("insert volume %q: %v", title, err)
	}
	for i, sid := range seriesIDs {
		isPrimary := i == 0
		if _, err := c.tdb.Exec(ctx, `
			INSERT INTO series_memberships (volume_id, series_id, is_primary, position)
			VALUES ($1, $2, $3, $4)`,
			id, sid, isPrimary, float64(i+1),
		); err != nil {
			t.Fatalf("insert membership (%s, %s): %v", id, sid, err)
		}
		c.volumeSeries[id] = append(c.volumeSeries[id], c.seriesLabels[sid])
	}
	for _, lid := range serverLabelIDs {
		c.mustAssignRawLabel(ctx, t, lid, domain.ItemTypeVolume, id)
	}
	c.volumeLabels[id] = slices.Clone(serverLabelIDs)
	c.allVolumeIDs = append(c.allVolumeIDs, id)
	return id
}

func (c *seededCatalogue) mustCreateAsset(ctx context.Context, t *testing.T, volumeID, filename string, serverLabelIDs ...string) string {
	t.Helper()
	id := domain.MustNewID()
	if _, err := c.tdb.Exec(ctx, `
		INSERT INTO media_assets (id, volume_id, library_root_id, rel_path_bytes, rel_path_display, rel_path_match, file_size, format, page_count)
		VALUES ($1, $2, $3, $4, $5, $5, 1024, 'CBZ', 20)`,
		id, volumeID, c.rootID, []byte(filename), filename,
	); err != nil {
		t.Fatalf("insert media_asset %q: %v", filename, err)
	}
	for _, lid := range serverLabelIDs {
		c.mustAssignRawLabel(ctx, t, lid, domain.ItemTypeAsset, id)
	}
	c.assetLabels[id] = slices.Clone(serverLabelIDs)
	c.assetVolume[id] = volumeID
	c.allAssetIDs = append(c.allAssetIDs, id)
	return id
}

func (c *seededCatalogue) queryVisibleIDsAndCount(
	ctx context.Context,
	t *testing.T,
	pred *postgres.LabelPredicate,
) (seriesIDs, volumeIDs, assetIDs, progressVolIDs []string) {
	t.Helper()

	// 1. Series list + count (testing startParam=2 combined with library_id=$1)
	sFrag, err := pred.SeriesFragment("s", 2)
	if err != nil {
		t.Fatalf("SeriesFragment: %v", err)
	}
	sArgs := append([]any{c.libraryID}, sFrag.Args...)
	seriesIDs = c.queryIDs(ctx, t, fmt.Sprintf(`SELECT s.id FROM series s WHERE s.library_id = $1 AND %s ORDER BY s.id`, sFrag.SQL), sArgs...)
	sCount := c.queryCount(ctx, t, fmt.Sprintf(`SELECT COUNT(*) FROM series s WHERE s.library_id = $1 AND %s`, sFrag.SQL), sArgs...)
	if int(sCount) != len(seriesIDs) {
		t.Fatalf("series COUNT(*) = %d != len(seriesIDs) = %d", sCount, len(seriesIDs))
	}

	// 2. Volumes list + count (testing startParam=1)
	vFrag, err := pred.VolumeFragment("v", 1)
	if err != nil {
		t.Fatalf("VolumeFragment: %v", err)
	}
	volumeIDs = c.queryIDs(ctx, t, fmt.Sprintf(`SELECT v.id FROM volumes v WHERE %s ORDER BY v.id`, vFrag.SQL), vFrag.Args...)
	vCount := c.queryCount(ctx, t, fmt.Sprintf(`SELECT COUNT(*) FROM volumes v WHERE %s`, vFrag.SQL), vFrag.Args...)
	if int(vCount) != len(volumeIDs) {
		t.Fatalf("volumes COUNT(*) = %d != len(volumeIDs) = %d", vCount, len(volumeIDs))
	}

	// 3. Assets list + count
	aFrag, err := pred.AssetFragment("a", 1)
	if err != nil {
		t.Fatalf("AssetFragment: %v", err)
	}
	assetIDs = c.queryIDs(ctx, t, fmt.Sprintf(`SELECT a.id FROM media_assets a WHERE %s ORDER BY a.id`, aFrag.SQL), aFrag.Args...)
	aCount := c.queryCount(ctx, t, fmt.Sprintf(`SELECT COUNT(*) FROM media_assets a WHERE %s`, aFrag.SQL), aFrag.Args...)
	if int(aCount) != len(assetIDs) {
		t.Fatalf("media_assets COUNT(*) = %d != len(assetIDs) = %d", aCount, len(assetIDs))
	}

	// 4. Reading progress list + count (testing startParam=2 combined with user_id=$1)
	rpFrag, err := pred.ReadingProgressFragment("rp", 2)
	if err != nil {
		t.Fatalf("ReadingProgressFragment: %v", err)
	}
	rpArgs := append([]any{c.progressUser}, rpFrag.Args...)
	progressVolIDs = c.queryIDs(ctx, t, fmt.Sprintf(`SELECT rp.volume_id FROM reading_progress rp WHERE rp.user_id = $1 AND %s ORDER BY rp.volume_id`, rpFrag.SQL), rpArgs...)
	rpCount := c.queryCount(ctx, t, fmt.Sprintf(`SELECT COUNT(*) FROM reading_progress rp WHERE rp.user_id = $1 AND %s`, rpFrag.SQL), rpArgs...)
	if int(rpCount) != len(progressVolIDs) {
		t.Fatalf("reading_progress COUNT(*) = %d != len(progressVolIDs) = %d", rpCount, len(progressVolIDs))
	}

	return seriesIDs, volumeIDs, assetIDs, progressVolIDs
}

func (c *seededCatalogue) queryIDs(ctx context.Context, t *testing.T, sql string, args ...any) []string {
	t.Helper()
	rows, err := c.tdb.Query(ctx, sql, args...)
	if err != nil {
		t.Fatalf("queryIDs failed:\nSQL: %s\nerr: %v", sql, err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err: %v", err)
	}
	return ids
}

func (c *seededCatalogue) queryCount(ctx context.Context, t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := c.tdb.QueryRow(ctx, sql, args...).Scan(&count); err != nil {
		t.Fatalf("queryCount failed:\nSQL: %s\nerr: %v", sql, err)
	}
	return count
}

func (c *seededCatalogue) domainReferenceVisible(
	principal *domain.Principal,
	userPolicies []domain.UserLabelPolicy,
	groupPolicies []domain.GroupLabelPolicy,
	includeTrashed bool,
) (wantSeries, wantVolumes, wantAssets, wantProgress []string) {
	effectiveUser := slices.Clone(userPolicies)
	effectiveGroup := slices.Clone(groupPolicies)
	allowTrash := includeTrashed && principal != nil && principal.HasPermission(domain.PermissionTrashPurge)
	if !allowTrash && principal != nil {
		effectiveGroup = append(effectiveGroup, domain.GroupLabelPolicy{
			GroupClaim: "__trash_filter__",
			LabelID:    domain.LabelMarkedForDeletionID,
			Effect:     domain.PolicyEffectRestrict,
		})
	}
	refPolicy := domain.NewEffectiveLabelPolicyForPrincipal(principal, effectiveUser, effectiveGroup)

	for _, sid := range c.allSeriesIDs {
		if refPolicy.EvaluateSeries(c.seriesLabels[sid]) {
			wantSeries = append(wantSeries, sid)
		}
	}
	for _, vid := range c.allVolumeIDs {
		if refPolicy.EvaluateVolume(c.volumeLabels[vid], c.volumeSeries[vid]) {
			wantVolumes = append(wantVolumes, vid)
		}
	}
	for _, aid := range c.allAssetIDs {
		vid := c.assetVolume[aid]
		if refPolicy.EvaluateAsset(c.assetLabels[aid], c.volumeLabels[vid], c.volumeSeries[vid]) {
			wantAssets = append(wantAssets, aid)
		}
	}
	for _, vid := range c.progressVols {
		if refPolicy.EvaluateVolume(c.volumeLabels[vid], c.volumeSeries[vid]) {
			wantProgress = append(wantProgress, vid)
		}
	}
	sort.Strings(wantProgress)
	return wantSeries, wantVolumes, wantAssets, wantProgress
}

func TestLabelPredicate_SeededCatalogueMatrix(t *testing.T) {
	ctx := context.Background()
	cat := setupSeededCatalogue(t)

	adminPrincipal := domain.NewPrincipal(*cat.adminUser, []domain.Role{cat.adminRole}, domain.CredentialKindPassword, "c-admin", nil)
	adminReadPAT := domain.NewPrincipal(*cat.adminUser, []domain.Role{cat.adminRole}, domain.CredentialKindPAT, "c-admin-pat", []domain.PATScope{domain.ScopeRead})
	readerPrincipal := domain.NewPrincipal(*cat.readerUser, []domain.Role{cat.readerRole}, domain.CredentialKindPassword, "c-reader", nil)
	restrictedPrincipal := domain.NewPrincipal(*cat.restrictedUser, []domain.Role{cat.restrictedRole}, domain.CredentialKindPassword, "c-rest", nil)
	multiPrincipal := domain.NewPrincipal(*cat.multiRoleUser, []domain.Role{cat.readerRole, cat.restrictedRole}, domain.CredentialKindPassword, "c-multi", nil)

	type matrixCase struct {
		name            string
		principal       *domain.Principal
		userPolicies    []domain.UserLabelPolicy
		groupPolicies   []domain.GroupLabelPolicy
		includeTrashed  bool
		persistPolicies bool
		verify          func(t *testing.T, seriesIDs, volumeIDs, assetIDs, progressIDs []string)
	}

	assertContains := func(t *testing.T, kind string, got []string, wantPresent []string, wantAbsent []string) {
		t.Helper()
		for _, id := range wantPresent {
			if !slices.Contains(got, id) {
				t.Errorf("%s: expected %s to be VISIBLE, got %v", kind, id, got)
			}
		}
		for _, id := range wantAbsent {
			if slices.Contains(got, id) {
				t.Errorf("%s: expected %s to be HIDDEN, got %v", kind, id, got)
			}
		}
	}

	cases := []matrixCase{
		{
			name:           "nil principal fails closed even with IncludeTrashed",
			principal:      nil,
			includeTrashed: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
					t.Fatalf("nil principal must see nothing; got s=%d v=%d a=%d rp=%d", len(s), len(v), len(a), len(rp))
				}
			},
		},
		{
			name:            "admin normal view hides trashed items",
			principal:       adminPrincipal,
			includeTrashed:  false,
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, _ []string) {
				assertContains(t, "series", s, []string{cat.sClean, cat.sKids, cat.sNSFW, cat.sUserKidsOnly}, []string{cat.sTrashed})
				assertContains(t, "volumes", v, []string{cat.vInClean, cat.vInKids, cat.vAllowedVolInClean, cat.vAltPathRestrictedSec, cat.vZeroAssetsInKids}, []string{cat.vInTrashedSeries, cat.vTrashedInKids, cat.vStandaloneTrashed})
				assertContains(t, "assets", a, []string{cat.aKidsClean, cat.aKidsNSFW}, []string{cat.aKidsTrashed, cat.aInTrashedSeries, cat.aInTrashedVol})
			},
		},
		{
			name:            "admin trash view with PermissionTrashPurge sees trashed items",
			principal:       adminPrincipal,
			includeTrashed:  true,
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, _ []string) {
				if len(s) != len(cat.allSeriesIDs) || len(v) != len(cat.allVolumeIDs) || len(a) != len(cat.allAssetIDs) {
					t.Fatalf("expected all items visible in admin trash view; got s=%d/%d v=%d/%d a=%d/%d",
						len(s), len(cat.allSeriesIDs), len(v), len(cat.allVolumeIDs), len(a), len(cat.allAssetIDs))
				}
			},
		},
		{
			name:           "admin with read-only PAT lacks PermissionTrashPurge so trash stays hidden",
			principal:      adminReadPAT,
			includeTrashed: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				assertContains(t, "series", s, []string{cat.sClean, cat.sKids}, []string{cat.sTrashed})
				assertContains(t, "volumes", v, []string{cat.vInKids}, []string{cat.vInTrashedSeries, cat.vTrashedInKids, cat.vStandaloneTrashed})
				assertContains(t, "assets", a, []string{cat.aKidsClean}, []string{cat.aKidsTrashed, cat.aInTrashedSeries, cat.aInTrashedVol})
				assertContains(t, "progress", rp, []string{cat.vInKids}, []string{cat.vTrashedInKids})
			},
		},
		{
			name:            "reader with zero policy rows allows all non-trashed content",
			principal:       readerPrincipal,
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, _ []string) {
				assertContains(t, "series", s, []string{cat.sClean, cat.sKids, cat.sNSFW, cat.sUserKidsOnly}, []string{cat.sTrashed})
				assertContains(t, "volumes", v, []string{cat.vInClean, cat.vStandaloneUnlabeled, cat.vZeroAssetsInKids}, []string{cat.vInTrashedSeries, cat.vTrashedInKids, cat.vStandaloneTrashed})
				assertContains(t, "assets", a, []string{cat.aKidsClean, cat.aKidsNSFW}, []string{cat.aKidsTrashed})
			},
		},
		{
			name:      "reader blacklist mode (RESTRICT nsfw): alternate path cannot bypass, asset restriction hides one variant",
			principal: readerPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				assertContains(t, "series", s,
					[]string{cat.sClean, cat.sKids, cat.sAllAges, cat.sUserKidsOnly},
					[]string{cat.sNSFW, cat.sKidsNSFW, cat.sTrashed},
				)
				assertContains(t, "volumes", v,
					[]string{cat.vInClean, cat.vInKids, cat.vAllowedVolInClean, cat.vInKidsAndClean, cat.vStandaloneUnlabeled, cat.vStandaloneKids, cat.vZeroAssetsInKids},
					[]string{cat.vInKidsVolRestricted, cat.vAltPathRestrictedPrimary, cat.vAltPathRestrictedSec, cat.vStandaloneNSFW, cat.vStandaloneKidsNSFW, cat.vInTrashedSeries, cat.vTrashedInKids, cat.vStandaloneTrashed},
				)
				// Single-variant asset restriction hides only aKidsNSFW / aStandaloneNSFW, keeping clean variants visible.
				// Volume/series restrictions hide ALL variants including clean/kids-tagged assets inside them.
				assertContains(t, "assets", a,
					[]string{cat.aKidsClean, cat.aStandaloneClean, cat.aInClean, cat.aInKidsAndClean},
					[]string{cat.aKidsNSFW, cat.aKidsTrashed, cat.aStandaloneNSFW, cat.aCleanInRestrictedVol, cat.aKidsInRestrictedVol, cat.aInAltRestricted, cat.aAltPrimaryRestricted},
				)
				assertContains(t, "progress", rp,
					[]string{cat.vInKids, cat.vAllowedVolInClean, cat.vStandaloneKids},
					[]string{cat.vInKidsVolRestricted, cat.vAltPathRestrictedSec, cat.vTrashedInKids},
				)
			},
		},
		{
			name:      "reader whitelist mode (ALLOW kids): top-down I-4 hides allowed vol/asset in non-allowed series",
			principal: readerPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.readerUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				assertContains(t, "series", s,
					[]string{cat.sKids, cat.sKidsNSFW},
					[]string{cat.sClean, cat.sAllAges, cat.sNSFW, cat.sTrashed, cat.sUserKidsOnly},
				)
				assertContains(t, "volumes", v,
					[]string{cat.vInKids, cat.vInKidsAndClean, cat.vStandaloneKids, cat.vZeroAssetsInKids, cat.vAltPathRestrictedPrimary, cat.vAltPathRestrictedSec},
					[]string{cat.vInClean, cat.vAllowedVolInClean, cat.vStandaloneUnlabeled, cat.vInTrashedSeries, cat.vTrashedInKids, cat.vStandaloneTrashed},
				)
				assertContains(t, "assets", a,
					[]string{cat.aKidsClean, cat.aStandaloneClean, cat.aInKidsAndClean},
					[]string{cat.aAllowedInHiddenVol, cat.aAllowedInUnallowedStandal, cat.aInClean, cat.aKidsTrashed},
				)
				assertContains(t, "progress", rp,
					[]string{cat.vInKids, cat.vStandaloneKids},
					[]string{cat.vAllowedVolInClean, cat.vTrashedInKids},
				)
			},
		},
		{
			name:      "reader combined ALLOW kids + RESTRICT nsfw",
			principal: readerPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.readerUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
				{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				assertContains(t, "series", s,
					[]string{cat.sKids},
					[]string{cat.sClean, cat.sAllAges, cat.sNSFW, cat.sKidsNSFW, cat.sTrashed, cat.sUserKidsOnly},
				)
				assertContains(t, "volumes", v,
					[]string{cat.vInKids, cat.vInKidsAndClean, cat.vStandaloneKids, cat.vZeroAssetsInKids},
					[]string{cat.vAllowedVolInClean, cat.vInKidsVolRestricted, cat.vAltPathRestrictedPrimary, cat.vAltPathRestrictedSec, cat.vStandaloneKidsNSFW, cat.vStandaloneUnlabeled, cat.vTrashedInKids},
				)
				assertContains(t, "assets", a,
					[]string{cat.aKidsClean, cat.aStandaloneClean, cat.aInKidsAndClean},
					[]string{cat.aKidsNSFW, cat.aKidsTrashed, cat.aStandaloneNSFW, cat.aAllowedInHiddenVol, cat.aCleanInRestrictedVol, cat.aKidsInRestrictedVol, cat.aInAltRestricted, cat.aAltPrimaryRestricted, cat.aAllowedInUnallowedStandal},
				)
				assertContains(t, "progress", rp,
					[]string{cat.vInKids, cat.vStandaloneKids},
					[]string{cat.vInKidsVolRestricted, cat.vAltPathRestrictedSec, cat.vAllowedVolInClean, cat.vTrashedInKids},
				)
			},
		},
		{
			name:            "Restricted reader with zero policies fails closed (I-5) even with IncludeTrashed",
			principal:       restrictedPrincipal,
			includeTrashed:  true,
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
					t.Fatalf("Restricted reader with 0 policies must see nothing; got s=%v v=%v a=%v rp=%v", s, v, a, rp)
				}
			},
		},
		{
			name:      "Restricted reader whose only ALLOW is cancelled by group RESTRICT fails closed (I-5)",
			principal: restrictedPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			},
			groupPolicies: []domain.GroupLabelPolicy{
				{GroupClaim: "kids", LabelID: cat.lblKids, Effect: domain.PolicyEffectRestrict},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
					t.Fatalf("Restricted reader with cancelled ALLOW must see nothing; got s=%v v=%v a=%v rp=%v", s, v, a, rp)
				}
			},
		},
		{
			name:      "Restricted reader whose group ALLOW is cancelled by user RESTRICT fails closed (I-5)",
			principal: restrictedPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectRestrict},
			},
			groupPolicies: []domain.GroupLabelPolicy{
				{GroupClaim: "kids", LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
					t.Fatalf("Restricted reader with cancelled group ALLOW must see nothing; got s=%v v=%v a=%v rp=%v", s, v, a, rp)
				}
			},
		},
		{
			name:      "Restricted reader with group ALLOW kids + user RESTRICT nsfw",
			principal: restrictedPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.restrictedUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
			},
			groupPolicies: []domain.GroupLabelPolicy{
				{GroupClaim: "family", LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, _ []string) {
				assertContains(t, "series", s, []string{cat.sKids}, []string{cat.sClean, cat.sNSFW, cat.sKidsNSFW, cat.sTrashed, cat.sUserKidsOnly})
				assertContains(t, "volumes", v, []string{cat.vInKids, cat.vStandaloneKids}, []string{cat.vAllowedVolInClean, cat.vAltPathRestrictedSec, cat.vInKidsVolRestricted})
				assertContains(t, "assets", a, []string{cat.aKidsClean, cat.aStandaloneClean}, []string{cat.aKidsNSFW, cat.aAllowedInHiddenVol, cat.aAllowedInUnallowedStandal})
			},
		},
		{
			name:      "Restricted reader with user ALLOW kids + group ALLOW all-ages + group RESTRICT nsfw",
			principal: restrictedPrincipal,
			userPolicies: []domain.UserLabelPolicy{
				{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			},
			groupPolicies: []domain.GroupLabelPolicy{
				{GroupClaim: "family", LabelID: cat.lblAllAges, Effect: domain.PolicyEffectAllow},
				{GroupClaim: "safe", LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
			},
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, _ []string) {
				assertContains(t, "series", s, []string{cat.sKids, cat.sAllAges}, []string{cat.sClean, cat.sNSFW, cat.sKidsNSFW, cat.sTrashed})
				assertContains(t, "volumes", v, []string{cat.vInKids, cat.vInAllAges, cat.vStandaloneKids}, []string{cat.vAllowedVolInClean, cat.vAltPathRestrictedSec})
				assertContains(t, "assets", a, []string{cat.aKidsClean, cat.aInAllAges, cat.aStandaloneClean}, []string{cat.aKidsNSFW, cat.aAllowedInHiddenVol})
			},
		},
		{
			name:            "multi-role user holding Reader + Restricted reader with zero policies fails closed",
			principal:       multiPrincipal,
			persistPolicies: true,
			verify: func(t *testing.T, s, v, a, rp []string) {
				if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
					t.Fatalf("multi-role with requires_allow_labels must fail closed; got s=%d v=%d a=%d rp=%d", len(s), len(v), len(a), len(rp))
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := postgres.LabelPredicateOptions{IncludeTrashed: tc.includeTrashed}
			pred := postgres.NewLabelPredicate(tc.principal, tc.userPolicies, tc.groupPolicies, opts)

			gotS, gotV, gotA, gotRP := cat.queryVisibleIDsAndCount(ctx, t, pred)
			wantS, wantV, wantA, wantRP := cat.domainReferenceVisible(tc.principal, tc.userPolicies, tc.groupPolicies, tc.includeTrashed)

			if !slices.Equal(gotS, wantS) {
				t.Errorf("Series mismatch vs domain.EvaluateSeries:\n got:  %v\n want: %v", gotS, wantS)
			}
			if !slices.Equal(gotV, wantV) {
				t.Errorf("Volume mismatch vs domain.EvaluateVolume:\n got:  %v\n want: %v", gotV, wantV)
			}
			if !slices.Equal(gotA, wantA) {
				t.Errorf("Asset mismatch vs domain.EvaluateAsset:\n got:  %v\n want: %v", gotA, wantA)
			}
			if !slices.Equal(gotRP, wantRP) {
				t.Errorf("ReadingProgress mismatch:\n got:  %v\n want: %v", gotRP, wantRP)
			}

			tc.verify(t, gotS, gotV, gotA, gotRP)

			if tc.persistPolicies && tc.principal != nil {
				// Persist user_label_policies in PostgreSQL and test LoadLabelPredicate + LabelRepository.LabelPredicate.
				if _, err := cat.tdb.Exec(ctx, `DELETE FROM user_label_policies WHERE user_id = $1`, tc.principal.User.ID); err != nil {
					t.Fatalf("clearing policies: %v", err)
				}
				for _, up := range tc.userPolicies {
					if err := cat.labelRepo.SetUserLabelPolicy(ctx, &up); err != nil {
						t.Fatalf("SetUserLabelPolicy: %v", err)
					}
				}

				loadedPred, err := postgres.LoadLabelPredicate(ctx, cat.tdb.Pool, tc.principal, tc.groupPolicies, opts)
				if err != nil {
					t.Fatalf("LoadLabelPredicate: %v", err)
				}
				lS, lV, lA, lRP := cat.queryVisibleIDsAndCount(ctx, t, loadedPred)
				if !slices.Equal(lS, gotS) || !slices.Equal(lV, gotV) || !slices.Equal(lA, gotA) || !slices.Equal(lRP, gotRP) {
					t.Errorf("LoadLabelPredicate diverged from NewLabelPredicate")
				}

				repoPred, err := cat.labelRepo.LabelPredicate(ctx, tc.principal, tc.groupPolicies, opts)
				if err != nil {
					t.Fatalf("LabelRepository.LabelPredicate: %v", err)
				}
				rS, rV, rA, rRP := cat.queryVisibleIDsAndCount(ctx, t, repoPred)
				if !slices.Equal(rS, gotS) || !slices.Equal(rV, gotV) || !slices.Equal(rA, gotA) || !slices.Equal(rRP, gotRP) {
					t.Errorf("LabelRepository.LabelPredicate diverged from NewLabelPredicate")
				}
			}
		})
	}

	t.Run("renaming referenced SERVER labels preserves visibility", func(t *testing.T) {
		if _, err := cat.tdb.Exec(ctx, `DELETE FROM user_label_policies WHERE user_id = $1`, cat.readerUser.ID); err != nil {
			t.Fatalf("clear policies: %v", err)
		}
		for _, up := range []domain.UserLabelPolicy{
			{UserID: cat.readerUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
		} {
			if err := cat.labelRepo.SetUserLabelPolicy(ctx, &up); err != nil {
				t.Fatalf("SetUserLabelPolicy: %v", err)
			}
		}

		beforePred, err := cat.labelRepo.LabelPredicate(ctx, readerPrincipal, nil, postgres.LabelPredicateOptions{})
		if err != nil {
			t.Fatalf("LabelPredicate before rename: %v", err)
		}
		bS, bV, bA, bRP := cat.queryVisibleIDsAndCount(ctx, t, beforePred)

		if _, err := cat.labelRepo.RenameLabel(ctx, cat.lblKids, "children-renamed"); err != nil {
			t.Fatalf("RenameLabel(kids): %v", err)
		}
		if _, err := cat.labelRepo.RenameLabel(ctx, cat.lblNSFW, "adult-renamed"); err != nil {
			t.Fatalf("RenameLabel(nsfw): %v", err)
		}

		afterPred, err := cat.labelRepo.LabelPredicate(ctx, readerPrincipal, nil, postgres.LabelPredicateOptions{})
		if err != nil {
			t.Fatalf("LabelPredicate after rename: %v", err)
		}
		aS, aV, aA, aRP := cat.queryVisibleIDsAndCount(ctx, t, afterPred)

		if !slices.Equal(bS, aS) || !slices.Equal(bV, aV) || !slices.Equal(bA, aA) || !slices.Equal(bRP, aRP) {
			t.Fatalf("renaming SERVER labels changed visibility:\nbefore: s=%v v=%v a=%v rp=%v\nafter:  s=%v v=%v a=%v rp=%v",
				bS, bV, bA, bRP, aS, aV, aA, aRP)
		}
	})

	t.Run("Reader with explicit ALLOW marked_for_deletion and IncludeTrashed=false sees zero rows", func(t *testing.T) {
		pred := postgres.NewLabelPredicate(
			readerPrincipal,
			[]domain.UserLabelPolicy{
				{UserID: cat.readerUser.ID, LabelID: domain.LabelMarkedForDeletionID, Effect: domain.PolicyEffectAllow},
			},
			nil,
			postgres.LabelPredicateOptions{IncludeTrashed: false},
		)
		s, v, a, rp := cat.queryVisibleIDsAndCount(ctx, t, pred)
		if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
			t.Fatalf("expected 0 rows when ALLOW marked_for_deletion conflicts with implicit trash filter; got s=%v v=%v a=%v rp=%v", s, v, a, rp)
		}
	})

	t.Run("vInKids asset variant fallback with SelectPreferredAsset", func(t *testing.T) {
		vInKidsAssets := func(pred *postgres.LabelPredicate) []string {
			frag, err := pred.AssetFragment("a", 2)
			if err != nil {
				t.Fatalf("AssetFragment: %v", err)
			}
			args := append([]any{cat.vInKids}, frag.Args...)
			sql := `SELECT a.id FROM media_assets a WHERE a.volume_id = $1 AND (` + frag.SQL + `) ORDER BY a.id`
			return cat.queryIDs(ctx, t, sql, args...)
		}

		adminTrash := postgres.NewLabelPredicate(adminPrincipal, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: true})
		allVariants := vInKidsAssets(adminTrash)
		wantAll := []string{cat.aKidsClean, cat.aKidsNSFW, cat.aKidsTrashed}
		sort.Strings(wantAll)
		if !slices.Equal(allVariants, wantAll) {
			t.Fatalf("admin trash view variants = %v, want %v", allVariants, wantAll)
		}

		adminNormal := postgres.NewLabelPredicate(adminPrincipal, nil, nil, postgres.LabelPredicateOptions{IncludeTrashed: false})
		normalVariants := vInKidsAssets(adminNormal)
		wantNormal := []string{cat.aKidsClean, cat.aKidsNSFW}
		sort.Strings(wantNormal)
		if !slices.Equal(normalVariants, wantNormal) {
			t.Fatalf("admin normal view variants = %v, want %v", normalVariants, wantNormal)
		}

		restrictedReader := postgres.NewLabelPredicate(
			readerPrincipal,
			[]domain.UserLabelPolicy{
				{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
			},
			nil,
			postgres.LabelPredicateOptions{IncludeTrashed: false},
		)
		permitted := vInKidsAssets(restrictedReader)
		if !slices.Equal(permitted, []string{cat.aKidsClean}) {
			t.Fatalf("restricted reader vInKids variants = %v, want [%s]", permitted, cat.aKidsClean)
		}

		vol := domain.Volume{
			ID:               cat.vInKids,
			PreferredAssetID: &cat.aKidsNSFW,
			Assets: []domain.MediaAsset{
				{ID: cat.aKidsNSFW, VolumeID: cat.vInKids},
				{ID: cat.aKidsTrashed, VolumeID: cat.vInKids},
				{ID: cat.aKidsClean, VolumeID: cat.vInKids},
			},
		}
		chosen, ok := vol.SelectPreferredAsset("", func(a domain.MediaAsset) bool {
			return slices.Contains(permitted, a.ID)
		})
		if !ok || chosen == nil || chosen.ID != cat.aKidsClean {
			t.Fatalf("SelectPreferredAsset = (%v, %v), want %s", chosen, ok, cat.aKidsClean)
		}
	})

	t.Run("zero-value LabelPredicate denies all rows in PostgreSQL", func(t *testing.T) {
		var zero postgres.LabelPredicate
		s, v, a, rp := cat.queryVisibleIDsAndCount(ctx, t, &zero)
		if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
			t.Fatalf("zero-value predicate must return 0 rows across all entities; got s=%v v=%v a=%v rp=%v", s, v, a, rp)
		}
	})

	t.Run("outer aliases matching subquery names do not shadow row references in PostgreSQL", func(t *testing.T) {
		predicates := []struct {
			name string
			pred *postgres.LabelPredicate
		}{
			{
				name: "restrict-only",
				pred: postgres.NewLabelPredicate(
					readerPrincipal,
					[]domain.UserLabelPolicy{
						{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
					},
					nil,
					postgres.LabelPredicateOptions{},
				),
			},
			{
				name: "allow+restrict",
				pred: postgres.NewLabelPredicate(
					restrictedPrincipal,
					[]domain.UserLabelPolicy{
						{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
						{UserID: cat.restrictedUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectRestrict},
					},
					nil,
					postgres.LabelPredicateOptions{},
				),
			},
		}

		outerAliases := []string{
			"l", "il", "sm",
			"L", "IL", "SM",
			"_lp_l", "_lp_il", "_lp_sm",
			"labels", "item_labels", "series_memberships",
		}

		for _, tc := range predicates {
			t.Run(tc.name, func(t *testing.T) {
				wantS, wantV, wantA, wantRP := cat.queryVisibleIDsAndCount(ctx, t, tc.pred)
				for _, alias := range outerAliases {
					sFrag, err := tc.pred.SeriesFragment(alias, 2)
					if err != nil {
						t.Fatalf("SeriesFragment(%q): %v", alias, err)
					}
					gotS := cat.queryIDs(ctx, t,
						fmt.Sprintf(`SELECT %s.id FROM series %s WHERE %s.library_id = $1 AND %s ORDER BY %s.id`, alias, alias, alias, sFrag.SQL, alias),
						append([]any{cat.libraryID}, sFrag.Args...)...)
					if !slices.Equal(gotS, wantS) {
						t.Fatalf("SeriesFragment(%q) = %v, want %v", alias, gotS, wantS)
					}

					vFrag, err := tc.pred.VolumeFragment(alias, 1)
					if err != nil {
						t.Fatalf("VolumeFragment(%q): %v", alias, err)
					}
					gotV := cat.queryIDs(ctx, t,
						fmt.Sprintf(`SELECT %s.id FROM volumes %s WHERE %s ORDER BY %s.id`, alias, alias, vFrag.SQL, alias),
						vFrag.Args...)
					if !slices.Equal(gotV, wantV) {
						t.Fatalf("VolumeFragment(%q) = %v, want %v", alias, gotV, wantV)
					}

					aFrag, err := tc.pred.AssetFragment(alias, 1)
					if err != nil {
						t.Fatalf("AssetFragment(%q): %v", alias, err)
					}
					gotA := cat.queryIDs(ctx, t,
						fmt.Sprintf(`SELECT %s.id FROM media_assets %s WHERE %s ORDER BY %s.id`, alias, alias, aFrag.SQL, alias),
						aFrag.Args...)
					if !slices.Equal(gotA, wantA) {
						t.Fatalf("AssetFragment(%q) = %v, want %v", alias, gotA, wantA)
					}

					rpFrag, err := tc.pred.ReadingProgressFragment(alias, 2)
					if err != nil {
						t.Fatalf("ReadingProgressFragment(%q): %v", alias, err)
					}
					gotRP := cat.queryIDs(ctx, t,
						fmt.Sprintf(`SELECT %s.volume_id FROM reading_progress %s WHERE %s.user_id = $1 AND %s ORDER BY %s.volume_id`, alias, alias, alias, rpFrag.SQL, alias),
						append([]any{cat.progressUser}, rpFrag.Args...)...)
					if !slices.Equal(gotRP, wantRP) {
						t.Fatalf("ReadingProgressFragment(%q) = %v, want %v", alias, gotRP, wantRP)
					}
				}
			})
		}
	})

	t.Run("foreign user_label_policies rows are ignored in PostgreSQL", func(t *testing.T) {
		foreignOnly := postgres.NewLabelPredicate(
			restrictedPrincipal,
			[]domain.UserLabelPolicy{
				{UserID: cat.readerUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
			},
			nil,
			postgres.LabelPredicateOptions{},
		)
		s, v, a, rp := cat.queryVisibleIDsAndCount(ctx, t, foreignOnly)
		if len(s) != 0 || len(v) != 0 || len(a) != 0 || len(rp) != 0 {
			t.Fatalf("Restricted reader must see 0 rows with only foreign ALLOW rows; got s=%v v=%v a=%v rp=%v", s, v, a, rp)
		}

		mixed := postgres.NewLabelPredicate(
			restrictedPrincipal,
			[]domain.UserLabelPolicy{
				{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow},
				{UserID: cat.readerUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectRestrict},
				{UserID: cat.readerUser.ID, LabelID: cat.lblNSFW, Effect: domain.PolicyEffectAllow},
			},
			nil,
			postgres.LabelPredicateOptions{},
		)
		wantS, wantV, wantA, wantRP := cat.domainReferenceVisible(
			restrictedPrincipal,
			[]domain.UserLabelPolicy{{UserID: cat.restrictedUser.ID, LabelID: cat.lblKids, Effect: domain.PolicyEffectAllow}},
			nil,
			false,
		)
		gotS, gotV, gotA, gotRP := cat.queryVisibleIDsAndCount(ctx, t, mixed)
		if !slices.Equal(gotS, wantS) || !slices.Equal(gotV, wantV) || !slices.Equal(gotA, wantA) || !slices.Equal(gotRP, wantRP) {
			t.Fatalf("foreign user policies affected SQL visibility:\ngot:  s=%v v=%v a=%v rp=%v\nwant: s=%v v=%v a=%v rp=%v",
				gotS, gotV, gotA, gotRP, wantS, wantV, wantA, wantRP)
		}
	})
}
