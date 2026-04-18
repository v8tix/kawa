package tests

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/v8tix/kawa/middleware"
)

// ── LoggingTransport ──────────────────────────────────────────────────────────

func TestNewLoggingTransport_NilArgsFallbackToDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		logger *slog.Logger
		next   http.RoundTripper
	}{
		{"nil logger uses default", nil, okTransport()},
		{"nil next uses http.DefaultTransport", slog.Default(), nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if lt := middleware.NewLoggingTransport(tc.logger, tc.next); lt == nil {
				t.Fatal("expected non-nil LoggingTransport")
			}
		})
	}
}

func TestLoggingTransport_RoundTripSuccess(t *testing.T) {
	t.Parallel()
	lt := middleware.NewLoggingTransport(slog.Default(), okTransport())
	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	resp, err := lt.RoundTrip(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestLoggingTransport_RoundTripError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("dial tcp: connection refused")
	lt := middleware.NewLoggingTransport(slog.Default(), errTransport(sentinel))
	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	_, err := lt.RoundTrip(req)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error, got: %v", err)
	}
}

func TestLoggingTransport_WithLevelOptionsDoNotPanic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		transport func() *middleware.LoggingTransport
		wantErr   bool
	}{
		{
			name: "WithLevel on success path",
			transport: func() *middleware.LoggingTransport {
				return middleware.NewLoggingTransport(slog.Default(), okTransport()).
					WithLevel(slog.LevelDebug)
			},
		},
		{
			name: "WithErrorLevel on error path",
			transport: func() *middleware.LoggingTransport {
				return middleware.NewLoggingTransport(slog.Default(), errTransport(errors.New("network error"))).
					WithErrorLevel(slog.LevelWarn)
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
			_, err := tc.transport().RoundTrip(req)
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// ── CustomHeaders ─────────────────────────────────────────────────────────────

func TestCustomHeaders_InjectsHeadersIntoRequest(t *testing.T) {
	t.Parallel()
	var capturedReq *http.Request
	next := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		capturedReq = r
		rec := httptest.NewRecorder()
		rec.WriteHeader(http.StatusOK)
		return rec.Result(), nil
	})

	ch := middleware.NewCustomHeaders(map[string]string{
		"X-Api-Key":  "secret",
		"X-Trace-Id": "abc123",
	}, next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	if _, err := ch.RoundTrip(req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := capturedReq.Header.Get("X-Api-Key"); got != "secret" {
		t.Errorf("X-Api-Key: got %q, want %q", got, "secret")
	}
	if got := capturedReq.Header.Get("X-Trace-Id"); got != "abc123" {
		t.Errorf("X-Trace-Id: got %q, want %q", got, "abc123")
	}
}

func TestCustomHeaders_NilNextUsesDefaultTransport(t *testing.T) {
	t.Parallel()
	_ = middleware.NewCustomHeaders(map[string]string{"X-Test": "1"}, nil)
}

func TestCustomHeaders_DoesNotMutateOriginalRequest(t *testing.T) {
	t.Parallel()
	ch := middleware.NewCustomHeaders(map[string]string{"X-Injected": "yes"}, okTransport())
	req := httptest.NewRequest(http.MethodGet, "http://example.com", nil)
	if _, err := ch.RoundTrip(req); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := req.Header.Get("X-Injected"); got != "" {
		t.Errorf("original request was mutated: X-Injected = %q", got)
	}
}
