package comicinfo_test

import (
	"context"
	"errors"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
	"github.com/inexcession/tobiten/internal/metadata/comicinfo"
	"github.com/inexcession/tobiten/internal/storage"
	"github.com/inexcession/tobiten/internal/storage/postgres"
	"github.com/inexcession/tobiten/internal/storage/postgres/migrations"
	"github.com/inexcession/tobiten/internal/testdb"
)

func TestMain(m *testing.M) {
	os.Exit(testdb.Run(m))
}

func setupImporterTestDB(t *testing.T) (*testdb.TestDB, *postgres.Transactor) {
	t.Helper()
	db := testdb.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := migrations.RunFS(ctx, db, migrations.CoreFS); err != nil {
		t.Fatalf("setupImporterTestDB: applying migrations: %v", err)
	}
	return db, postgres.NewTransactor(db.Pool)
}

func seedTestSeriesAndVolume(
	ctx context.Context,
	t *testing.T,
	tx storage.Tx,
) (domain.Series, domain.Volume) {
	t.Helper()
	lib := domain.Library{
		Name: "Comics",
		Mode: domain.LibraryModeVirtual,
	}
	if err := tx.Libraries().CreateLibrary(ctx, &lib); err != nil {
		t.Fatalf("CreateLibrary: %v", err)
	}

	series := domain.Series{
		LibraryID: lib.ID,
		Title:     "Test Series",
		TitleSort: "Test Series",
		Status:    domain.SeriesStatusOngoing,
	}
	if err := tx.CatalogueWrites().CreateSeries(ctx, &series); err != nil {
		t.Fatalf("CreateSeries: %v", err)
	}

	num := 1.0
	vol := domain.Volume{
		LibraryID: lib.ID,
		Kind:      domain.VolumeKindVolume,
		Number:    &num,
	}
	if err := tx.CatalogueWrites().CreateVolume(ctx, &vol); err != nil {
		t.Fatalf("CreateVolume: %v", err)
	}

	pos := 1.0
	if err := tx.SeriesMemberships().SetVolumeMemberships(ctx, vol.ID, []domain.SeriesMembership{
		{
			VolumeID:  vol.ID,
			SeriesID:  series.ID,
			IsPrimary: true,
			Kind:      domain.MembershipKindSeries,
			Position:  &pos,
			Source:    domain.MembershipSourceResolver,
		},
	}); err != nil {
		t.Fatalf("SetVolumeMemberships: %v", err)
	}

	return series, vol
}

func parseStrictXML(t *testing.T, xmlBody string) comicinfo.Metadata {
	t.Helper()
	meta, diags, err := comicinfo.Parse(strings.NewReader(xmlBody), comicinfo.Strict)
	if err != nil {
		t.Fatalf("comicinfo.Parse failed: %v (diags: %+v)", err, diags)
	}
	if len(diags) != 0 {
		t.Fatalf("comicinfo.Parse unexpected diagnostics: %+v", diags)
	}
	return meta
}

type tableXmins struct {
	people         map[string]string
	entities       map[string]string
	credits        map[string]string
	volumeEntities map[string]string
	volume         string
}

func readTableXmins(ctx context.Context, t *testing.T, db *testdb.TestDB, volumeID string) tableXmins {
	t.Helper()
	out := tableXmins{
		people:         make(map[string]string),
		entities:       make(map[string]string),
		credits:        make(map[string]string),
		volumeEntities: make(map[string]string),
	}

	readMap := func(query string, dest map[string]string) {
		t.Helper()
		rows, err := db.Query(ctx, query)
		if err != nil {
			t.Fatalf("query %q: %v", query, err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, xmin string
			if err := rows.Scan(&id, &xmin); err != nil {
				t.Fatalf("scan %q: %v", query, err)
			}
			dest[id] = xmin
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows %q: %v", query, err)
		}
	}

	readMap(`SELECT id, xmin::text FROM people ORDER BY id`, out.people)
	readMap(`SELECT id, xmin::text FROM entities ORDER BY id`, out.entities)
	readMap(`SELECT id, xmin::text FROM credits ORDER BY id`, out.credits)
	readMap(`SELECT volume_id || ':' || entity_id, xmin::text FROM volume_entities ORDER BY volume_id, entity_id`, out.volumeEntities)

	if err := db.QueryRow(ctx, `SELECT xmin::text FROM volumes WHERE id = $1`, volumeID).Scan(&out.volume); err != nil {
		t.Fatalf("query volume xmin: %v", err)
	}
	return out
}

func assertTableXminsEqual(t *testing.T, before, after tableXmins) {
	t.Helper()
	if !maps.Equal(before.people, after.people) {
		t.Fatalf("people rows changed: before=%v after=%v", before.people, after.people)
	}
	if !maps.Equal(before.entities, after.entities) {
		t.Fatalf("entities rows changed: before=%v after=%v", before.entities, after.entities)
	}
	if !maps.Equal(before.credits, after.credits) {
		t.Fatalf("credits rows changed: before=%v after=%v", before.credits, after.credits)
	}
	if !maps.Equal(before.volumeEntities, after.volumeEntities) {
		t.Fatalf("volume_entities rows changed: before=%v after=%v", before.volumeEntities, after.volumeEntities)
	}
	if before.volume != after.volume {
		t.Fatalf("volume row changed: before=%s after=%s", before.volume, after.volume)
	}
}

func TestImportVolumeCreditsAndEntities_AllEightRolesAndThreeKindsFromParsedXML(t *testing.T) {
	t.Parallel()
	db, transactor := setupImporterTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	xmlDoc := `<ComicInfo>
		<Series>Test Series</Series>
		<Number>1</Number>
		<Writer>Alice, Bob, Carol</Writer>
		<Penciller>Dave, Alice</Penciller>
		<Inker>Eve</Inker>
		<Colorist>Frank, Grace</Colorist>
		<Letterer>Heidi</Letterer>
		<CoverArtist>Ivan, Alice</CoverArtist>
		<Editor>Judy</Editor>
		<Translator>Kenji, Mika</Translator>
		<Characters>Phoenix, Wolverine</Characters>
		<Teams>Phoenix, X-Men</Teams>
		<Locations>Phoenix, Krakoa</Locations>
	</ComicInfo>`
	meta := parseStrictXML(t, xmlDoc)

	var (
		series domain.Series
		vol    domain.Volume
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		series, vol = seedTestSeriesAndVolume(ctx, t, tx)
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, meta)
	})
	if err != nil {
		t.Fatalf("WithTx import failed: %v", err)
	}

	creditRepo := postgres.NewCreditRepository(db.Pool)
	seriesCredits, err := creditRepo.ListCreditsBySeries(ctx, series.ID)
	if err != nil {
		t.Fatalf("ListCreditsBySeries: %v", err)
	}
	if len(seriesCredits) != 0 {
		t.Fatalf("expected zero series-level credits synthesized, got %+v", seriesCredits)
	}

	volCredits, err := creditRepo.ListCreditsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}

	peopleRepo := postgres.NewPersonRepository(db.Pool)
	type gotCredit struct {
		name     string
		personID string
		pos      int
	}
	byRole := make(map[domain.CreditRole][]gotCredit)
	for _, c := range volCredits {
		if c.SeriesID != nil || c.VolumeID == nil || *c.VolumeID != vol.ID {
			t.Fatalf("unexpected credit target: %+v", c)
		}
		p, err := peopleRepo.GetPersonByID(ctx, c.PersonID)
		if err != nil {
			t.Fatalf("GetPersonByID(%s): %v", c.PersonID, err)
		}
		byRole[c.Role] = append(byRole[c.Role], gotCredit{name: p.Name, personID: p.ID, pos: c.Position})
	}

	wantRoles := map[domain.CreditRole][]string{
		domain.CreditRoleWriter:      {"Alice", "Bob", "Carol"},
		domain.CreditRolePenciller:   {"Dave", "Alice"},
		domain.CreditRoleInker:       {"Eve"},
		domain.CreditRoleColorist:    {"Frank", "Grace"},
		domain.CreditRoleLetterer:    {"Heidi"},
		domain.CreditRoleCoverArtist: {"Ivan", "Alice"},
		domain.CreditRoleEditor:      {"Judy"},
		domain.CreditRoleTranslator:  {"Kenji", "Mika"},
	}

	if len(byRole) != len(wantRoles) {
		t.Fatalf("mapped %d credit roles, want %d (%v)", len(byRole), len(wantRoles), byRole)
	}
	for role, wantNames := range wantRoles {
		got := byRole[role]
		if len(got) != len(wantNames) {
			t.Fatalf("role %s count = %d, want %d (%+v)", role, len(got), len(wantNames), got)
		}
		for i, wantName := range wantNames {
			if got[i].name != wantName || got[i].pos != i {
				t.Fatalf("role %s[%d] = %+v, want name=%q pos=%d", role, i, got[i], wantName, i)
			}
		}
	}

	// Same person across Writer, Penciller, and CoverArtist shares one Person ID.
	aliceWriterID := byRole[domain.CreditRoleWriter][0].personID
	alicePencillerID := byRole[domain.CreditRolePenciller][1].personID
	aliceCoverID := byRole[domain.CreditRoleCoverArtist][1].personID
	if aliceWriterID != alicePencillerID || aliceWriterID != aliceCoverID {
		t.Fatalf("expected Alice to share one person ID across roles, got writer=%s penciller=%s cover=%s",
			aliceWriterID, alicePencillerID, aliceCoverID)
	}

	entRepo := postgres.NewEntityRepository(db.Pool)
	links, err := entRepo.ListVolumeEntities(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}

	gotEntities := make(map[domain.EntityKind][]string)
	phoenixIDs := make(map[domain.EntityKind]string)
	for _, link := range links {
		ent, err := entRepo.GetEntityByID(ctx, link.EntityID)
		if err != nil {
			t.Fatalf("GetEntityByID(%s): %v", link.EntityID, err)
		}
		gotEntities[ent.Kind] = append(gotEntities[ent.Kind], ent.Name)
		if ent.Name == "Phoenix" {
			phoenixIDs[ent.Kind] = ent.ID
		}
	}
	for kind := range gotEntities {
		slices.Sort(gotEntities[kind])
	}

	if !slices.Equal(gotEntities[domain.EntityKindCharacter], []string{"Phoenix", "Wolverine"}) {
		t.Fatalf("characters = %v, want [Phoenix Wolverine]", gotEntities[domain.EntityKindCharacter])
	}
	if !slices.Equal(gotEntities[domain.EntityKindTeam], []string{"Phoenix", "X-Men"}) {
		t.Fatalf("teams = %v, want [Phoenix X-Men]", gotEntities[domain.EntityKindTeam])
	}
	if !slices.Equal(gotEntities[domain.EntityKindLocation], []string{"Krakoa", "Phoenix"}) {
		t.Fatalf("locations = %v, want [Krakoa Phoenix]", gotEntities[domain.EntityKindLocation])
	}
	if len(phoenixIDs) != 3 ||
		phoenixIDs[domain.EntityKindCharacter] == phoenixIDs[domain.EntityKindTeam] ||
		phoenixIDs[domain.EntityKindCharacter] == phoenixIDs[domain.EntityKindLocation] ||
		phoenixIDs[domain.EntityKindTeam] == phoenixIDs[domain.EntityKindLocation] {
		t.Fatalf("expected distinct entity IDs per kind for Phoenix, got %v", phoenixIDs)
	}
}

func TestImportVolumeCreditsAndEntities_CommaSplittingAndDuplicateDeduplication(t *testing.T) {
	t.Parallel()
	db, transactor := setupImporterTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Exercise raw comma-separated strings in Metadata directly, whitespace, empty items,
	// case-folded duplicates, and NFC vs NFD Unicode duplicates.
	meta := comicinfo.Metadata{
		Writer:     []string{"  \u00c9lodie Dupont , , e\u0301lodie dupont ", " Bob , bob "},
		Characters: []string{"  Hero One , hero one , , Hero Two "},
	}

	var vol domain.Volume
	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		_, vol = seedTestSeriesAndVolume(ctx, t, tx)
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, meta)
	})
	if err != nil {
		t.Fatalf("WithTx import failed: %v", err)
	}

	credits, err := postgres.NewCreditRepository(db.Pool).ListCreditsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	if len(credits) != 2 {
		t.Fatalf("len(credits) = %d, want 2 (%+v)", len(credits), credits)
	}

	peopleRepo := postgres.NewPersonRepository(db.Pool)
	p0, err := peopleRepo.GetPersonByID(ctx, credits[0].PersonID)
	if err != nil {
		t.Fatalf("GetPersonByID(0): %v", err)
	}
	p1, err := peopleRepo.GetPersonByID(ctx, credits[1].PersonID)
	if err != nil {
		t.Fatalf("GetPersonByID(1): %v", err)
	}
	if p0.Name != "\u00c9lodie Dupont" || credits[0].Position != 0 {
		t.Fatalf("credit[0] = (%q, pos=%d), want (Élodie Dupont, pos=0)", p0.Name, credits[0].Position)
	}
	if p1.Name != "Bob" || credits[1].Position != 1 {
		t.Fatalf("credit[1] = (%q, pos=%d), want (Bob, pos=1)", p1.Name, credits[1].Position)
	}

	links, err := postgres.NewEntityRepository(db.Pool).ListVolumeEntities(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}
	if len(links) != 2 {
		t.Fatalf("len(volume_entities) = %d, want 2", len(links))
	}
}

func TestImportVolumeCreditsAndEntities_IdempotentReimportChangesNoRows(t *testing.T) {
	t.Parallel()
	db, transactor := setupImporterTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	xmlDoc := `<ComicInfo>
		<Writer>Alice, Bob</Writer>
		<Penciller>Carol</Penciller>
		<Inker>Dave</Inker>
		<Colorist>Eve</Colorist>
		<Letterer>Frank</Letterer>
		<CoverArtist>Grace</CoverArtist>
		<Editor>Heidi</Editor>
		<Translator>Ivan</Translator>
		<Characters>Luffy, Zoro</Characters>
		<Teams>Straw Hat Pirates</Teams>
		<Locations>East Blue</Locations>
	</ComicInfo>`
	meta := parseStrictXML(t, xmlDoc)

	var vol domain.Volume
	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		_, vol = seedTestSeriesAndVolume(ctx, t, tx)
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, meta)
	})
	if err != nil {
		t.Fatalf("initial WithTx import: %v", err)
	}

	before := readTableXmins(ctx, t, db, vol.ID)

	// Re-import exact same parsed input: zero rows may be inserted, updated, or deleted.
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, parseStrictXML(t, xmlDoc))
	})
	if err != nil {
		t.Fatalf("identical WithTx re-import: %v", err)
	}

	after := readTableXmins(ctx, t, db, vol.ID)
	assertTableXminsEqual(t, before, after)

	// Re-import with case/whitespace variants only: still reuses the same rows and writes nothing.
	variantXML := `<ComicInfo>
		<Writer>  alice ,  BOB </Writer>
		<Penciller>carol</Penciller>
		<Inker>dave</Inker>
		<Colorist>eve</Colorist>
		<Letterer>frank</Letterer>
		<CoverArtist>grace</CoverArtist>
		<Editor>heidi</Editor>
		<Translator>ivan</Translator>
		<Characters> luffy, zoro </Characters>
		<Teams>straw hat pirates</Teams>
		<Locations>east blue</Locations>
	</ComicInfo>`
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, parseStrictXML(t, variantXML))
	})
	if err != nil {
		t.Fatalf("variant WithTx re-import: %v", err)
	}

	afterVariant := readTableXmins(ctx, t, db, vol.ID)
	assertTableXminsEqual(t, before, afterVariant)
}

func TestImportVolumeCreditsAndEntities_SourceRemovalAndLockPreservation(t *testing.T) {
	t.Parallel()
	db, transactor := setupImporterTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	initialMeta := parseStrictXML(t, `<ComicInfo>
		<Writer>Alice, Bob, Carol</Writer>
		<Penciller>Dave, Eve</Penciller>
		<Translator>Frank</Translator>
		<Characters>Hero, Sidekick</Characters>
		<Teams>Squad</Teams>
		<Locations>City</Locations>
	</ComicInfo>`)

	var (
		vol           domain.Volume
		aliceCreditID string
		eveCreditID   string
		daveCreditID  string
		carolCreditID string
		alicePersonID string
		heroEntityID  string
		squadEntityID string
		lockedAlice   domain.Person
		lockedHero    domain.Entity
	)

	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		_, vol = seedTestSeriesAndVolume(ctx, t, tx)
		if err := comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, initialMeta); err != nil {
			return err
		}

		// Lock Alice's WRITER credit, Eve's PENCILLER credit, and Alice's curated person name.
		credits, err := tx.Credits().ListCreditsByVolume(ctx, vol.ID)
		if err != nil {
			return err
		}
		for i := range credits {
			c := &credits[i]
			p, err := tx.People().GetPersonByID(ctx, c.PersonID)
			if err != nil {
				return err
			}
			switch {
			case c.Role == domain.CreditRoleWriter && p.Name == "Alice":
				c.LockedFields = domain.FieldLocks{"credit": true}
				if err := tx.Credits().UpdateCredit(ctx, c); err != nil {
					return err
				}
				aliceCreditID = c.ID
				alicePersonID = p.ID
				p.Name = "Alice Curated"
				p.LockedFields = domain.FieldLocks{"name": true}
				if err := tx.People().UpdatePerson(ctx, p); err != nil {
					return err
				}
				lockedAlice = *p
			case c.Role == domain.CreditRoleWriter && p.Name == "Carol":
				carolCreditID = c.ID
			case c.Role == domain.CreditRolePenciller && p.Name == "Dave":
				daveCreditID = c.ID
			case c.Role == domain.CreditRolePenciller && p.Name == "Eve":
				c.LockedFields = domain.FieldLocks{"position": true}
				if err := tx.Credits().UpdateCredit(ctx, c); err != nil {
					return err
				}
				eveCreditID = c.ID
			}
		}

		// Lock Hero's volume_entity link and curated entity name.
		links, err := tx.Entities().ListVolumeEntities(ctx, vol.ID)
		if err != nil {
			return err
		}
		for i := range links {
			link := &links[i]
			e, err := tx.Entities().GetEntityByID(ctx, link.EntityID)
			if err != nil {
				return err
			}
			switch e.Name {
			case "Hero":
				link.LockedFields = domain.FieldLocks{"entity": true}
				if err := tx.Entities().SetVolumeEntities(ctx, vol.ID, links); err != nil {
					return err
				}
				heroEntityID = e.ID
				e.Name = "Hero Curated"
				e.LockedFields = domain.FieldLocks{"name": true}
				if err := tx.Entities().UpdateEntity(ctx, e); err != nil {
					return err
				}
				lockedHero = *e
			case "Squad":
				squadEntityID = e.ID
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed and lock setup: %v", err)
	}

	before := readTableXmins(ctx, t, db, vol.ID)

	// Re-import:
	// - Writer loses Alice (locked -> kept) and Bob (unlocked -> removed); Carol remains (pos 2 -> 0)
	// - Penciller keeps Dave (pos 0 unchanged) and loses Eve (locked -> kept at pos 1), adds Grace (pos 1)
	// - Translator loses Frank (unlocked -> removed)
	// - Characters loses Hero (locked -> kept) and Sidekick (unlocked -> removed)
	// - Teams keeps Squad (unchanged)
	// - Locations replaces City (unlocked -> removed) with Island (added)
	updatedMeta := parseStrictXML(t, `<ComicInfo>
		<Writer>Carol</Writer>
		<Penciller>Dave, Grace</Penciller>
		<Teams>Squad</Teams>
		<Locations>Island</Locations>
	</ComicInfo>`)

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		return comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, updatedMeta)
	})
	if err != nil {
		t.Fatalf("updated WithTx import: %v", err)
	}

	after := readTableXmins(ctx, t, db, vol.ID)

	// Locked rows and unchanged rows keep their exact xmin.
	if after.credits[aliceCreditID] != before.credits[aliceCreditID] {
		t.Fatalf("locked Alice credit row was rewritten: before=%s after=%s", before.credits[aliceCreditID], after.credits[aliceCreditID])
	}
	if after.credits[eveCreditID] != before.credits[eveCreditID] {
		t.Fatalf("locked Eve credit row was rewritten: before=%s after=%s", before.credits[eveCreditID], after.credits[eveCreditID])
	}
	if after.credits[daveCreditID] != before.credits[daveCreditID] {
		t.Fatalf("unchanged Dave credit row was rewritten: before=%s after=%s", before.credits[daveCreditID], after.credits[daveCreditID])
	}
	if after.people[alicePersonID] != before.people[alicePersonID] {
		t.Fatalf("locked Alice person row was rewritten: before=%s after=%s", before.people[alicePersonID], after.people[alicePersonID])
	}
	heroLinkKey := vol.ID + ":" + heroEntityID
	squadLinkKey := vol.ID + ":" + squadEntityID
	if after.volumeEntities[heroLinkKey] != before.volumeEntities[heroLinkKey] {
		t.Fatalf("locked Hero volume_entity row was rewritten: before=%s after=%s", before.volumeEntities[heroLinkKey], after.volumeEntities[heroLinkKey])
	}
	if after.volumeEntities[squadLinkKey] != before.volumeEntities[squadLinkKey] {
		t.Fatalf("unchanged Squad volume_entity row was rewritten: before=%s after=%s", before.volumeEntities[squadLinkKey], after.volumeEntities[squadLinkKey])
	}

	gotCredits, err := postgres.NewCreditRepository(db.Pool).ListCreditsByVolume(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListCreditsByVolume: %v", err)
	}
	if len(gotCredits) != 5 {
		t.Fatalf("len(gotCredits) = %d, want 5 (Alice locked, Carol, Dave, Eve locked, Grace): %+v", len(gotCredits), gotCredits)
	}

	peopleRepo := postgres.NewPersonRepository(db.Pool)
	type creditSummary struct {
		id     string
		name   string
		role   domain.CreditRole
		pos    int
		locked domain.FieldLocks
	}
	gotByID := make(map[string]creditSummary, len(gotCredits))
	for _, c := range gotCredits {
		p, err := peopleRepo.GetPersonByID(ctx, c.PersonID)
		if err != nil {
			t.Fatalf("GetPersonByID(%s): %v", c.PersonID, err)
		}
		gotByID[c.ID] = creditSummary{id: c.ID, name: p.Name, role: c.Role, pos: c.Position, locked: c.LockedFields}
	}

	if got := gotByID[aliceCreditID]; got.name != lockedAlice.Name || got.role != domain.CreditRoleWriter || got.pos != 0 || !maps.Equal(got.locked, domain.FieldLocks{"credit": true}) {
		t.Fatalf("locked Alice credit altered: %+v", got)
	}
	if got := gotByID[carolCreditID]; got.name != "Carol" || got.role != domain.CreditRoleWriter || got.pos != 0 || len(got.locked) != 0 {
		t.Fatalf("Carol credit unexpected: %+v", got)
	}
	if got := gotByID[daveCreditID]; got.name != "Dave" || got.role != domain.CreditRolePenciller || got.pos != 0 {
		t.Fatalf("Dave credit unexpected: %+v", got)
	}
	if got := gotByID[eveCreditID]; got.name != "Eve" || got.role != domain.CreditRolePenciller || got.pos != 1 || !maps.Equal(got.locked, domain.FieldLocks{"position": true}) {
		t.Fatalf("locked Eve credit altered: %+v", got)
	}

	foundGrace := false
	for _, s := range gotByID {
		if s.name == "Bob" || s.name == "Frank" {
			t.Fatalf("unlocked disappearing credit still present: %+v", s)
		}
		if s.name == "Grace" {
			foundGrace = true
			if s.role != domain.CreditRolePenciller || s.pos != 1 {
				t.Fatalf("Grace credit = %+v, want role=PENCILLER pos=1", s)
			}
		}
	}
	if !foundGrace {
		t.Fatal("Grace credit missing after re-import")
	}

	// Verify locked person and entity metadata survived.
	gotAlice, err := peopleRepo.GetPersonByID(ctx, alicePersonID)
	if err != nil {
		t.Fatalf("GetPersonByID(Alice): %v", err)
	}
	if gotAlice.Name != lockedAlice.Name || !maps.Equal(gotAlice.LockedFields, lockedAlice.LockedFields) {
		t.Fatalf("locked Alice person altered: %+v", gotAlice)
	}

	entRepo := postgres.NewEntityRepository(db.Pool)
	gotHero, err := entRepo.GetEntityByID(ctx, heroEntityID)
	if err != nil {
		t.Fatalf("GetEntityByID(Hero): %v", err)
	}
	if gotHero.Name != lockedHero.Name || !maps.Equal(gotHero.LockedFields, lockedHero.LockedFields) {
		t.Fatalf("locked Hero entity altered: %+v", gotHero)
	}

	gotLinks, err := entRepo.ListVolumeEntities(ctx, vol.ID)
	if err != nil {
		t.Fatalf("ListVolumeEntities: %v", err)
	}
	if len(gotLinks) != 3 {
		t.Fatalf("len(gotLinks) = %d, want 3 (Hero locked, Squad, Island): %+v", len(gotLinks), gotLinks)
	}

	linkNames := make([]string, 0, len(gotLinks))
	for _, l := range gotLinks {
		e, err := entRepo.GetEntityByID(ctx, l.EntityID)
		if err != nil {
			t.Fatalf("GetEntityByID(%s): %v", l.EntityID, err)
		}
		linkNames = append(linkNames, string(e.Kind)+":"+e.Name)
		if l.EntityID == heroEntityID && !maps.Equal(l.LockedFields, domain.FieldLocks{"entity": true}) {
			t.Fatalf("locked Hero link lost lock: %+v", l)
		}
	}
	slices.Sort(linkNames)
	wantLinks := []string{"CHARACTER:Hero Curated", "LOCATION:Island", "TEAM:Squad"}
	if !slices.Equal(linkNames, wantLinks) {
		t.Fatalf("linked entities = %v, want %v", linkNames, wantLinks)
	}
}

func TestImportVolumeCreditsAndEntities_CallerTxRollbackAndValidation(t *testing.T) {
	t.Parallel()
	db, transactor := setupImporterTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Validation errors outside DB mutations.
	if err := comicinfo.ImportVolumeCreditsAndEntities(ctx, nil, "vol-1", comicinfo.Metadata{}); err == nil {
		t.Fatal("expected error for nil transaction")
	}

	canceledCtx, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if err := comicinfo.ImportVolumeCreditsAndEntities(canceledCtx, nil, "vol-1", comicinfo.Metadata{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx error = %v, want context.Canceled", err)
	}

	var vol domain.Volume
	err := transactor.WithTx(ctx, func(tx storage.Tx) error {
		_, vol = seedTestSeriesAndVolume(ctx, t, tx)
		return nil
	})
	if err != nil {
		t.Fatalf("seed volume: %v", err)
	}

	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, "  ", comicinfo.Metadata{}); !errors.Is(err, domain.ErrEmptyVolumeID) {
			t.Fatalf("empty volumeID error = %v, want ErrEmptyVolumeID", err)
		}
		return tx.WithTx(ctx, func(sub storage.Tx) error {
			err := comicinfo.ImportVolumeCreditsAndEntities(ctx, sub, "nonexistent-volume", comicinfo.Metadata{
				Writer:     []string{"Ghost Writer"},
				Characters: []string{"Ghost Character"},
			})
			if !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("unknown volumeID error = %v, want ErrNotFound", err)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatalf("validation check tx: %v", err)
	}

	// Caller transaction rollback leaves zero people, entities, credits, or links.
	sentinel := errors.New("abort caller transaction")
	err = transactor.WithTx(ctx, func(tx storage.Tx) error {
		if err := comicinfo.ImportVolumeCreditsAndEntities(ctx, tx, vol.ID, comicinfo.Metadata{
			Writer:     []string{"Rolled Back Writer"},
			Characters: []string{"Rolled Back Character"},
		}); err != nil {
			return err
		}
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTx error = %v, want sentinel", err)
	}

	var peopleCount, entityCount, creditCount, linkCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM people`).Scan(&peopleCount); err != nil {
		t.Fatalf("count people: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM entities`).Scan(&entityCount); err != nil {
		t.Fatalf("count entities: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM credits`).Scan(&creditCount); err != nil {
		t.Fatalf("count credits: %v", err)
	}
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM volume_entities`).Scan(&linkCount); err != nil {
		t.Fatalf("count volume_entities: %v", err)
	}
	if peopleCount != 0 || entityCount != 0 || creditCount != 0 || linkCount != 0 {
		t.Fatalf("rollback leaked rows: people=%d entities=%d credits=%d links=%d",
			peopleCount, entityCount, creditCount, linkCount)
	}
}
