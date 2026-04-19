package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

const SessionCookieName = "superegg_session"

type Manager struct {
	passwordHash []byte
	signingKey   []byte
	ttl          time.Duration
}

type sessionPayload struct {
	ExpiresAt int64  `json:"expires_at"`
	Subject   string `json:"subject"`
}

type contextKey string

const authContextKey contextKey = "auth.subject"

func New(passwordHash, sessionSecret string) (*Manager, error) {
	if passwordHash == "" {
		return nil, errors.New("missing admin password hash")
	}
	if sessionSecret == "" {
		return nil, errors.New("missing session secret")
	}

	return &Manager{
		passwordHash: []byte(passwordHash),
		signingKey:   []byte(sessionSecret),
		ttl:          30 * 24 * time.Hour,
	}, nil
}

func (m *Manager) CheckPassword(password string) error {
	return bcrypt.CompareHashAndPassword(m.passwordHash, []byte(password))
}

func (m *Manager) StartSession(w http.ResponseWriter) error {
	payload := sessionPayload{
		ExpiresAt: time.Now().Add(m.ttl).Unix(),
		Subject:   "admin",
	}
	value, err := m.sign(payload)
	if err != nil {
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(m.ttl.Seconds()),
	})
	return nil
}

func (m *Manager) EndSession(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}

func (m *Manager) Require(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject, err := m.AuthenticateRequest(r)
		if err != nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), authContextKey, subject)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *Manager) AuthenticateRequest(r *http.Request) (string, error) {
	ck, err := r.Cookie(SessionCookieName)
	if err != nil {
		return "", err
	}
	return m.verify(ck.Value)
}

func Subject(ctx context.Context) string {
	subject, _ := ctx.Value(authContextKey).(string)
	return subject
}

func (m *Manager) sign(payload sessionPayload) (string, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal session: %w", err)
	}

	encoded := base64.RawURLEncoding.EncodeToString(body)
	mac := hmac.New(sha256.New, m.signingKey)
	mac.Write([]byte(encoded))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return encoded + "." + sig, nil
}

func (m *Manager) verify(raw string) (string, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return "", errors.New("invalid session")
	}

	expectedMAC := hmac.New(sha256.New, m.signingKey)
	expectedMAC.Write([]byte(parts[0]))
	expected := expectedMAC.Sum(nil)

	got, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", errors.New("invalid session signature")
	}
	if !hmac.Equal(expected, got) {
		return "", errors.New("session signature mismatch")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", errors.New("invalid session payload")
	}

	var payload sessionPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return "", errors.New("invalid session json")
	}
	if payload.ExpiresAt < time.Now().Unix() {
		return "", errors.New("session expired")
	}
	if payload.Subject == "" {
		return "", errors.New("session missing subject")
	}
	return payload.Subject, nil
}
