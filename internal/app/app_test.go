package app

import (
	"context"
	"errors"
	"testing"

	"github.com/inexcession/tobiten/internal/domain"
)

func TestNewStubServices(t *testing.T) {
	services := NewStubServices()
	if services.Library == nil {
		t.Fatal("LibraryService is nil")
	}
	if services.Reading == nil {
		t.Fatal("ReadingService is nil")
	}
	if services.Curation == nil {
		t.Fatal("CurationService is nil")
	}
	if services.Admin == nil {
		t.Fatal("AdminService is nil")
	}
}

func TestStubLibraryService(t *testing.T) {
	svc := NewStubLibraryService()
	ctx := context.Background()

	libs, err := svc.ListLibraries(ctx, nil)
	if err != nil {
		t.Fatalf("ListLibraries error: %v", err)
	}
	if len(libs) == 0 {
		t.Errorf("expected at least 1 library, got %d", len(libs))
	}

	lib, err := svc.GetLibrary(ctx, nil, libs[0].ID)
	if err != nil {
		t.Fatalf("GetLibrary error: %v", err)
	}
	if lib.Name != libs[0].Name {
		t.Errorf("GetLibrary got %q, want %q", lib.Name, libs[0].Name)
	}

	_, err = svc.GetLibrary(ctx, nil, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}

	seriesList, err := svc.ListSeries(ctx, nil, SeriesFilter{})
	if err != nil {
		t.Fatalf("ListSeries error: %v", err)
	}
	if len(seriesList) == 0 {
		t.Errorf("expected at least 1 series, got %d", len(seriesList))
	}

	ser, err := svc.GetSeries(ctx, nil, seriesList[0].ID)
	if err != nil {
		t.Fatalf("GetSeries error: %v", err)
	}
	if ser.Title != seriesList[0].Title {
		t.Errorf("GetSeries got %q, want %q", ser.Title, seriesList[0].Title)
	}

	vols, err := svc.ListVolumes(ctx, nil, VolumeFilter{})
	if err != nil {
		t.Fatalf("ListVolumes error: %v", err)
	}
	if len(vols) == 0 {
		t.Errorf("expected at least 1 volume, got %d", len(vols))
	}

	vol, err := svc.GetVolume(ctx, nil, vols[0].ID)
	if err != nil {
		t.Fatalf("GetVolume error: %v", err)
	}
	if vol.Title != vols[0].Title {
		t.Errorf("GetVolume got %q, want %q", vol.Title, vols[0].Title)
	}

	jobID, err := svc.ScanLibrary(ctx, nil, "lib-1")
	if err != nil {
		t.Fatalf("ScanLibrary error: %v", err)
	}
	if jobID == "" {
		t.Errorf("expected job ID, got empty")
	}
}

func TestStubReadingService(t *testing.T) {
	svc := NewStubReadingService()
	ctx := context.Background()

	// Initial progress
	p, err := svc.GetProgress(ctx, nil, "vol-1")
	if err != nil {
		t.Fatalf("GetProgress error: %v", err)
	}
	if p.Completed {
		t.Errorf("expected uncompleted initially")
	}

	// Update progress
	completed := false
	updated, err := svc.UpdateProgress(ctx, nil, UpdateProgressRequest{
		VolumeID:         "vol-1",
		Completed:        &completed,
		TotalProgression: 0.5,
		Locator: Locator{
			Type: "page",
			Page: 50,
		},
	})
	if err != nil {
		t.Fatalf("UpdateProgress error: %v", err)
	}
	if updated.TotalProgression != 0.5 || updated.Locator.Page != 50 {
		t.Errorf("unexpected updated progress: %+v", updated)
	}

	// Mark as read
	read, err := svc.MarkAsRead(ctx, nil, "vol-1")
	if err != nil {
		t.Fatalf("MarkAsRead error: %v", err)
	}
	if !read.Completed {
		t.Errorf("expected completed=true after MarkAsRead")
	}

	// Mark as unread
	err = svc.MarkAsUnread(ctx, nil, "vol-1")
	if err != nil {
		t.Fatalf("MarkAsUnread error: %v", err)
	}
	unread, err := svc.GetProgress(ctx, nil, "vol-1")
	if err != nil {
		t.Fatalf("GetProgress error: %v", err)
	}
	if unread.Completed {
		t.Errorf("expected completed=false after MarkAsUnread")
	}
}

func TestStubCurationService(t *testing.T) {
	svc := NewStubCurationService()
	ctx := context.Background()

	cover, err := svc.GetCover(ctx, nil, "vol-1")
	if err != nil {
		t.Fatalf("GetCover error: %v", err)
	}
	if cover.ContentType != "image/jpeg" || len(cover.Data) == 0 {
		t.Errorf("unexpected cover: %+v", cover)
	}

	if err := svc.HidePage(ctx, nil, HidePageRequest{AssetID: "asset-1", PageIndex: 0, Suppressed: true}); err != nil {
		t.Fatalf("HidePage error: %v", err)
	}

	if err := svc.StageTrash(ctx, nil, StageTrashRequest{ItemType: domain.ItemTypeVolume, ItemID: "vol-trash-1"}); err != nil {
		t.Fatalf("StageTrash error: %v", err)
	}

	trash, err := svc.ListTrash(ctx, nil)
	if err != nil {
		t.Fatalf("ListTrash error: %v", err)
	}
	if len(trash) != 1 || trash[0].ID != "vol-trash-1" {
		t.Errorf("expected 1 trashed item vol-trash-1, got %+v", trash)
	}

	if err := svc.RestoreTrash(ctx, nil, "vol-trash-1"); err != nil {
		t.Fatalf("RestoreTrash error: %v", err)
	}
	trash, _ = svc.ListTrash(ctx, nil)
	if len(trash) != 0 {
		t.Errorf("expected 0 trashed items after restore, got %d", len(trash))
	}

	_ = svc.StageTrash(ctx, nil, StageTrashRequest{ItemType: domain.ItemTypeVolume, ItemID: "vol-purge-1"})
	jobID, err := svc.PurgeTrash(ctx, nil, PurgeTrashRequest{ItemIDs: []string{"vol-purge-1"}})
	if err != nil {
		t.Fatalf("PurgeTrash error: %v", err)
	}
	if jobID == "" {
		t.Errorf("expected non-empty purge job ID")
	}
	trash, _ = svc.ListTrash(ctx, nil)
	if len(trash) != 0 {
		t.Errorf("expected 0 trashed items after purge, got %d", len(trash))
	}
}

func TestStubAdminService(t *testing.T) {
	svc := NewStubAdminService()
	ctx := context.Background()

	sealed, err := svc.IsSetupSealed(ctx)
	if err != nil {
		t.Fatalf("IsSetupSealed error: %v", err)
	}
	if sealed {
		t.Errorf("expected unsealed initially")
	}

	// Setup initial admin
	adminUser, err := svc.SetupInitialAdmin(ctx, SetupAdminRequest{
		Username: "newadmin",
		Password: "password123",
	})
	if err != nil {
		t.Fatalf("SetupInitialAdmin error: %v", err)
	}
	if adminUser.Username != "newadmin" {
		t.Errorf("expected username newadmin, got %q", adminUser.Username)
	}

	// Second setup attempt should fail with ErrSetupSealed
	_, err = svc.SetupInitialAdmin(ctx, SetupAdminRequest{
		Username: "admin2",
		Password: "password123",
	})
	if !errors.Is(err, ErrSetupSealed) {
		t.Errorf("expected ErrSetupSealed, got %v", err)
	}

	// Authenticate
	p, err := svc.AuthenticateUser(ctx, "newadmin", "password123")
	if err != nil {
		t.Fatalf("AuthenticateUser error: %v", err)
	}
	if p.User.Username != "newadmin" {
		t.Errorf("expected principal username newadmin, got %q", p.User.Username)
	}

	// Authenticate with empty password fails
	_, err = svc.AuthenticateUser(ctx, "newadmin", "")
	if !errors.Is(err, ErrUnauthorized) {
		t.Errorf("expected ErrUnauthorized, got %v", err)
	}

	// Users list
	users, err := svc.ListUsers(ctx, p)
	if err != nil {
		t.Fatalf("ListUsers error: %v", err)
	}
	if len(users) < 2 {
		t.Errorf("expected at least 2 users, got %d", len(users))
	}

	// Create user
	created, err := svc.CreateUser(ctx, p, CreateUserRequest{Username: "reader1", Password: "pwd"})
	if err != nil {
		t.Fatalf("CreateUser error: %v", err)
	}
	if created.Username != "reader1" {
		t.Errorf("expected username reader1, got %q", created.Username)
	}

	// Get user
	gotUser, err := svc.GetUser(ctx, p, created.ID)
	if err != nil {
		t.Fatalf("GetUser error: %v", err)
	}
	if gotUser.Username != "reader1" {
		t.Errorf("GetUser got %q, want reader1", gotUser.Username)
	}

	// Delete user
	if err := svc.DeleteUser(ctx, p, created.ID); err != nil {
		t.Fatalf("DeleteUser error: %v", err)
	}

	// Create PAT
	patRes, err := svc.CreatePAT(ctx, p, CreatePATRequest{
		Label:  "cli-pat",
		Scopes: []domain.PATScope{domain.ScopeRead},
	})
	if err != nil {
		t.Fatalf("CreatePAT error: %v", err)
	}
	if patRes.Token == "" {
		t.Errorf("expected PAT token, got empty")
	}

	// Jobs
	jobs, err := svc.ListJobs(ctx, p)
	if err != nil {
		t.Fatalf("ListJobs error: %v", err)
	}
	if len(jobs) == 0 {
		t.Errorf("expected at least 1 job, got %d", len(jobs))
	}

	job, err := svc.GetJob(ctx, p, jobs[0].ID)
	if err != nil {
		t.Fatalf("GetJob error: %v", err)
	}
	if job.ID != jobs[0].ID {
		t.Errorf("GetJob got %q, want %q", job.ID, jobs[0].ID)
	}
}
