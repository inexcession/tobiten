package app

import (
	"github.com/inexcession/tobiten/internal/domain"
)

func requirePrincipal(principal *domain.Principal) error {
	if principal == nil {
		return ErrUnauthorized
	}
	return nil
}

// RequirePermission verifies that principal is authenticated and holds perm.
// It returns ErrUnauthorized if principal is nil, and ErrForbidden if perm is
// unrecognized or absent from principal's effective permissions (ADR-0006 §4).
func RequirePermission(principal *domain.Principal, perm domain.Permission) error {
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	if !perm.IsValid() || !principal.HasPermission(perm) {
		return ErrForbidden
	}
	return nil
}

// RequireAllPermissions verifies that principal is authenticated and holds every
// permission in perms. An empty perms slice fails closed with ErrForbidden.
func RequireAllPermissions(principal *domain.Principal, perms ...domain.Permission) error {
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	if len(perms) == 0 {
		return ErrForbidden
	}
	for _, perm := range perms {
		if !perm.IsValid() || !principal.HasPermission(perm) {
			return ErrForbidden
		}
	}
	return nil
}

// RequireAnyPermission verifies that principal is authenticated and holds at least
// one recognized permission in perms. An empty perms slice fails closed with ErrForbidden.
func RequireAnyPermission(principal *domain.Principal, perms ...domain.Permission) error {
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	for _, perm := range perms {
		if perm.IsValid() && principal.HasPermission(perm) {
			return nil
		}
	}
	return ErrForbidden
}

// Authorize enforces perm on principal before invoking fn.
// Denied calls return ErrUnauthorized or ErrForbidden without invoking fn.
func Authorize(principal *domain.Principal, perm domain.Permission, fn func() error) error {
	if err := RequirePermission(principal, perm); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	return fn()
}

// AuthorizeValue enforces perm on principal before invoking fn and returning its result.
// Denied calls return the zero value and ErrUnauthorized or ErrForbidden without invoking fn.
func AuthorizeValue[T any](principal *domain.Principal, perm domain.Permission, fn func() (T, error)) (T, error) {
	if err := RequirePermission(principal, perm); err != nil {
		var zero T
		return zero, err
	}
	if fn == nil {
		var zero T
		return zero, nil
	}
	return fn()
}

// AuthorizeAny enforces that principal holds at least one permission in perms before invoking fn.
// Denied calls return ErrUnauthorized or ErrForbidden without invoking fn.
func AuthorizeAny(principal *domain.Principal, perms []domain.Permission, fn func() error) error {
	if err := RequireAnyPermission(principal, perms...); err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	return fn()
}

// AuthorizeAnyValue enforces that principal holds at least one permission in perms before invoking fn.
// Denied calls return the zero value and ErrUnauthorized or ErrForbidden without invoking fn.
func AuthorizeAnyValue[T any](principal *domain.Principal, perms []domain.Permission, fn func() (T, error)) (T, error) {
	if err := RequireAnyPermission(principal, perms...); err != nil {
		var zero T
		return zero, err
	}
	if fn == nil {
		var zero T
		return zero, nil
	}
	return fn()
}

// RequirePATScopes verifies that principal is authenticated and holds every permission
// conferred by the requested PAT scopes. It returns ErrUnauthorized if principal is nil,
// ErrForbidden if principal has no permissions or lacks any permission mapped to scopes,
// and ErrInvalidInput if scopes is empty or contains an unrecognized scope.
func RequirePATScopes(principal *domain.Principal, scopes []domain.PATScope) error {
	if err := requirePrincipal(principal); err != nil {
		return err
	}
	if len(principal.Permissions) == 0 {
		return ErrForbidden
	}
	if len(scopes) == 0 {
		return ErrInvalidInput
	}
	for _, scope := range scopes {
		if !scope.IsValid() {
			return ErrInvalidInput
		}
	}
	return RequireAllPermissions(principal, domain.ScopesToPermissions(scopes)...)
}
