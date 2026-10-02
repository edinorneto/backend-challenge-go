package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/edinorneto/backend-challenge-go/internal/config"
	"github.com/edinorneto/backend-challenge-go/internal/observability"
)

type identityKey struct{}

// Realm roles that define the permission model of the API.
const (
	// RoleWalletInternal is granted only to the internal service account and
	// authorizes wallet operations (create, read, ledger, reconciliation).
	RoleWalletInternal = "wallet-internal"
	// RoleWageringProvider is granted to game providers and authorizes
	// submitting and querying their own wagering transactions.
	RoleWageringProvider = "wagering-provider"
)

type Identity struct {
	ProviderID string
	Subject    string
	Roles      map[string]struct{}
}

func (i Identity) HasRole(role string) bool {
	_, ok := i.Roles[role]
	return ok
}

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}

type Verifier struct {
	verifier      *oidc.IDTokenVerifier
	providerClaim string
}

func NewVerifier(cfg config.Config) (*Verifier, error) {
	if strings.TrimSpace(cfg.OIDCIssuerURL) == "" || strings.TrimSpace(cfg.OIDCJWKSURL) == "" {
		return nil, errors.New("OIDC issuer and JWKS URLs are required")
	}
	providerClaimName := cfg.OIDCProviderClaim
	if strings.TrimSpace(providerClaimName) == "" {
		providerClaimName = "provider_id"
	}
	keySet := oidc.NewRemoteKeySet(context.Background(), cfg.OIDCJWKSURL)
	oidcConfig := &oidc.Config{SkipIssuerCheck: false}
	if strings.TrimSpace(cfg.OIDCAudience) != "" {
		oidcConfig.ClientID = cfg.OIDCAudience
	}
	return &Verifier{
		verifier:      oidc.NewVerifier(cfg.OIDCIssuerURL, keySet, oidcConfig),
		providerClaim: providerClaimName,
	}, nil
}

func (v *Verifier) Verify(ctx context.Context, token string) (Identity, error) {
	if v == nil || v.verifier == nil {
		return Identity{}, errors.New("OIDC verifier is not configured")
	}
	idToken, err := v.verifier.Verify(ctx, token)
	if err != nil {
		return Identity{}, fmt.Errorf("verify OIDC token: %w", err)
	}
	var claims map[string]json.RawMessage
	if err := idToken.Claims(&claims); err != nil {
		return Identity{}, fmt.Errorf("read OIDC claims: %w", err)
	}
	var subject string
	_ = json.Unmarshal(claims["sub"], &subject)
	providerID, err := providerClaim(claims, v.providerClaim)
	if err != nil {
		return Identity{}, err
	}
	if subject == "" {
		return Identity{}, errors.New("OIDC subject claim is required")
	}
	var directRoles []string
	_ = json.Unmarshal(claims["roles"], &directRoles)
	var realmAccess struct {
		Roles []string `json:"roles"`
	}
	_ = json.Unmarshal(claims["realm_access"], &realmAccess)
	roles := make(map[string]struct{}, len(directRoles)+len(realmAccess.Roles))
	for _, role := range append(directRoles, realmAccess.Roles...) {
		roles[role] = struct{}{}
	}
	return Identity{ProviderID: providerID, Subject: subject, Roles: roles}, nil
}

func providerClaim(raw map[string]json.RawMessage, name string) (string, error) {
	value, ok := raw[name]
	if !ok {
		return "", fmt.Errorf("OIDC claim %q is required", name)
	}
	var providerID string
	if err := json.Unmarshal(value, &providerID); err != nil || strings.TrimSpace(providerID) == "" {
		return "", fmt.Errorf("OIDC claim %q must be a non-empty string", name)
	}
	return providerID, nil
}

type Middleware struct {
	verifier *Verifier
	logger   *observability.Logger
	metrics  *observability.Metrics
}

func NewMiddleware(verifier *Verifier) *Middleware {
	return &Middleware{verifier: verifier}
}

// WithObservability enables the authentication failure log and metric.
func (m *Middleware) WithObservability(logger *observability.Logger, metrics *observability.Metrics) *Middleware {
	m.logger = logger
	m.metrics = metrics
	return m
}

// reject answers an authentication or authorization failure and records it. The
// token and the verifier error message are never logged.
func (m *Middleware) reject(w http.ResponseWriter, r *http.Request, status int, reason string, err error) {
	if m.metrics != nil {
		m.metrics.IncLabeled("auth_failures_total", map[string]string{"reason": reason})
	}
	if m.logger != nil {
		fields := map[string]string{"result": reason, "route": r.Pattern}
		if err != nil {
			m.logger.Error(r.Context(), "authentication_failed", err, fields)
		} else {
			m.logger.Info(r.Context(), "authentication_failed", fields)
		}
	}
	http.Error(w, `{"error":"`+reason+`"}`, status)
}

// Require authenticates the request and demands every listed role.
func (m *Middleware) Require(next http.Handler, roles ...string) http.Handler {
	return m.authenticate(next, func(identity Identity) bool {
		for _, role := range roles {
			if !identity.HasRole(role) {
				return false
			}
		}
		return true
	})
}

// RequireAny authenticates the request and demands at least one listed role.
func (m *Middleware) RequireAny(next http.Handler, roles ...string) http.Handler {
	return m.authenticate(next, func(identity Identity) bool {
		for _, role := range roles {
			if identity.HasRole(role) {
				return true
			}
		}
		return len(roles) == 0
	})
}

func (m *Middleware) authenticate(next http.Handler, authorized func(Identity) bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if m == nil || m.verifier == nil {
			http.Error(w, `{"error":"authentication_unavailable"}`, http.StatusServiceUnavailable)
			return
		}
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, prefix) {
			m.reject(w, r, http.StatusUnauthorized, "authentication_required", nil)
			return
		}
		identity, err := m.verifier.Verify(r.Context(), strings.TrimSpace(strings.TrimPrefix(header, prefix)))
		if err != nil {
			m.reject(w, r, http.StatusUnauthorized, "invalid_token", err)
			return
		}
		if !authorized(identity) {
			m.reject(w, r, http.StatusForbidden, "forbidden", nil)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), identity)))
	})
}
