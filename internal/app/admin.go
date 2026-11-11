package app

import (
	"context"
	"time"

	"github.com/inexcession/tobiten/internal/domain"
)

// SetupAdminRequest defines parameters for first-run initial admin creation (ADR-0014 §1).
type SetupAdminRequest struct {
	SetupToken string `json:"setup_token"`
	Username   string `json:"username"`
	Password   string `json:"password"`
}

// CreateUserRequest defines parameters for creating a new named user account.
type CreateUserRequest struct {
	Username string   `json:"username"`
	Password string   `json:"password"`
	RoleIDs  []string `json:"role_ids,omitempty"`
}

// CreatePATRequest defines parameters for generating a personal access token (ADR-0006 §3).
type CreatePATRequest struct {
	Label     string            `json:"label"`
	Scopes    []domain.PATScope `json:"scopes"`
	ExpiresAt *time.Time        `json:"expires_at,omitempty"`
}

// PATCreationResult contains the created credential and plaintext secret token.
type PATCreationResult struct {
	Credential *domain.Credential `json:"credential"`
	Token      string             `json:"token"`
}

// JobInfo represents the status of an asynchronous background job (ADR-0017).
type JobInfo struct {
	ID          string     `json:"id"`
	Kind        string     `json:"kind"`
	State       string     `json:"state"`
	ProgressPct float64    `json:"progress_pct"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	Error       *string    `json:"error,omitempty"`
}

// AdminService defines use-cases for user administration, auth bootstrap, and job monitoring.
type AdminService interface {
	IsSetupSealed(ctx context.Context) (bool, error)
	SetupInitialAdmin(ctx context.Context, req SetupAdminRequest) (*domain.User, error)
	AuthenticateUser(ctx context.Context, username, password string) (*domain.Principal, error)
	ListUsers(ctx context.Context, principal *domain.Principal) ([]domain.User, error)
	GetUser(ctx context.Context, principal *domain.Principal, id string) (*domain.User, error)
	CreateUser(ctx context.Context, principal *domain.Principal, req CreateUserRequest) (*domain.User, error)
	DeleteUser(ctx context.Context, principal *domain.Principal, id string) error
	CreatePAT(ctx context.Context, principal *domain.Principal, req CreatePATRequest) (*PATCreationResult, error)
	RevokeCredential(ctx context.Context, principal *domain.Principal, credID string) error
	ListJobs(ctx context.Context, principal *domain.Principal) ([]JobInfo, error)
	GetJob(ctx context.Context, principal *domain.Principal, jobID string) (*JobInfo, error)
}
