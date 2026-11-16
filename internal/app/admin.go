package app

import (
	"context"
	"encoding/json"
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
	ID              string          `json:"id"`
	Kind            string          `json:"kind"`
	State           string          `json:"state"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	CancelRequested bool            `json:"cancel_requested"`
	ProgressPct     float64         `json:"progress_pct"`
	Checkpoint      json.RawMessage `json:"checkpoint"`
	CreatedBy       *string         `json:"created_by"`
	DedupeKey       *string         `json:"dedupe_key"`
	RunAfter        time.Time       `json:"run_after"`
	CreatedAt       time.Time       `json:"created_at"`
	StartedAt       *time.Time      `json:"started_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	FinishedAt      *time.Time      `json:"finished_at"`
	Error           *string         `json:"error"`
}

// JobService defines use-cases for querying and cancelling durable background jobs (ADR-0017 §2).
type JobService interface {
	ListJobs(ctx context.Context, principal *domain.Principal) ([]JobInfo, error)
	GetJob(ctx context.Context, principal *domain.Principal, jobID string) (*JobInfo, error)
	CancelJob(ctx context.Context, principal *domain.Principal, jobID string) (*JobInfo, error)
}

// SetupService implements the unauthenticated first-run bootstrap only.
type SetupService interface {
	IsSetupSealed(ctx context.Context) (bool, error)
	SetupInitialAdmin(ctx context.Context, req SetupAdminRequest) (*domain.User, error)
}

// BasicAuthenticator verifies username/password credentials and resolves a Principal.
type BasicAuthenticator interface {
	AuthenticateBasic(ctx context.Context, username, password, clientIP string) (*domain.Principal, error)
}

// PATAuthenticator verifies personal access tokens and resolves a Principal.
type PATAuthenticator interface {
	AuthenticatePAT(ctx context.Context, token, clientIP string) (*domain.Principal, error)
}

// NativeSessionCookieName is the HTTP cookie carrying the native tobiten session token.
const NativeSessionCookieName = "tobiten_session"

// LoginRequest defines parameters for password-backed session creation.
type LoginRequest struct {
	Username   string
	Password   string
	RemoteAddr string
}

// LoginResult contains the resolved principal and newly minted session credentials.
type LoginResult struct {
	Principal    *domain.Principal
	SessionToken string
	CSRFToken    string
	ExpiresAt    time.Time
}

// SessionService verifies credentials and creates native server-side sessions.
type SessionService interface {
	Login(ctx context.Context, req LoginRequest) (*LoginResult, error)
}

// SessionAuthenticator verifies native session cookie tokens and returns the live Principal
// together with the SHA-256 hex digest of the session's bound CSRF token.
type SessionAuthenticator interface {
	AuthenticateSession(ctx context.Context, token string) (*domain.Principal, string, error)
}

// AdminService defines use-cases for user administration, auth bootstrap, and job monitoring.
type AdminService interface {
	SetupService
	JobService
	AuthenticateUser(ctx context.Context, username, password string) (*domain.Principal, error)
	ListUsers(ctx context.Context, principal *domain.Principal) ([]domain.User, error)
	GetUser(ctx context.Context, principal *domain.Principal, id string) (*domain.User, error)
	CreateUser(ctx context.Context, principal *domain.Principal, req CreateUserRequest) (*domain.User, error)
	DeleteUser(ctx context.Context, principal *domain.Principal, id string) error
	CreatePAT(ctx context.Context, principal *domain.Principal, req CreatePATRequest) (*PATCreationResult, error)
	RevokeCredential(ctx context.Context, principal *domain.Principal, credID string) error
}
