package tests

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/v8tix/kawa"
)

// ── HTTPStatusPolicy ──────────────────────────────────────────────────────────

func TestHTTPStatusPolicy_DefaultCodeClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code          int
		wantPermanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusForbidden, true},
		{http.StatusNotFound, true},
		{http.StatusMethodNotAllowed, true},
		{http.StatusConflict, true},
		{http.StatusUnprocessableEntity, true},
		{http.StatusRequestTimeout, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusBadGateway, false},
		{http.StatusServiceUnavailable, false},
		{http.StatusGatewayTimeout, false},
	}

	policy := kawa.NewHTTPStatusPolicy()

	for _, tc := range tests {
		t.Run(http.StatusText(tc.code), func(t *testing.T) {
			t.Parallel()
			err := policy.Classify(kawa.ErrInvalidHTTPStatus{StatusCode: tc.code})
			_, isPermanent := errors.AsType[*backoff.PermanentError](err)
			if isPermanent != tc.wantPermanent {
				t.Errorf("status %d: wantPermanent=%v, got permanent=%v", tc.code, tc.wantPermanent, isPermanent)
			}
		})
	}
}

func TestHTTPStatusPolicy_OverrideClassification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policy        *kawa.HTTPStatusPolicy
		code          int
		wantPermanent bool
	}{
		{
			name:          "WithTransient makes 404 retryable",
			policy:        kawa.NewHTTPStatusPolicy().WithTransient(http.StatusNotFound),
			code:          http.StatusNotFound,
			wantPermanent: false,
		},
		{
			name:          "WithPermanent makes 503 non-retryable",
			policy:        kawa.NewHTTPStatusPolicy().WithPermanent(http.StatusServiceUnavailable),
			code:          http.StatusServiceUnavailable,
			wantPermanent: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.policy.Classify(kawa.ErrInvalidHTTPStatus{StatusCode: tc.code})
			_, isPermanent := errors.AsType[*backoff.PermanentError](err)
			if isPermanent != tc.wantPermanent {
				t.Errorf("wantPermanent=%v, got permanent=%v", tc.wantPermanent, isPermanent)
			}
		})
	}
}

func TestHTTPStatusPolicy_NonHTTPErrorIsAlwaysTransient(t *testing.T) {
	t.Parallel()
	networkErr := errors.New("dial tcp: connection refused")
	err := kawa.NewHTTPStatusPolicy().Classify(networkErr)
	if _, ok := errors.AsType[*backoff.PermanentError](err); ok {
		t.Error("expected non-HTTP error to be transient, but got Permanent")
	}
	if !errors.Is(err, networkErr) {
		t.Errorf("expected original error to be preserved, got: %v", err)
	}
}

func TestHTTPStatusPolicy_ZeroStatusCodeIsTransient(t *testing.T) {
	t.Parallel()
	err := kawa.NewHTTPStatusPolicy().Classify(kawa.ErrInvalidHTTPStatus{})
	if _, ok := errors.AsType[*backoff.PermanentError](err); ok {
		t.Error("expected zero-StatusCode error to be transient, but got Permanent")
	}
}

func TestHTTPStatusPolicy_WithTransientChaining(t *testing.T) {
	t.Parallel()

	policy := kawa.NewHTTPStatusPolicy().
		WithTransient(http.StatusNotFound, http.StatusConflict).
		WithPermanent(http.StatusServiceUnavailable)

	cases := []struct {
		code      int
		permanent bool
	}{
		{http.StatusNotFound, false},
		{http.StatusConflict, false},
		{http.StatusServiceUnavailable, true},
		{http.StatusInternalServerError, false},
		{http.StatusBadRequest, true},
	}

	for _, tc := range cases {
		err := policy.Classify(kawa.ErrInvalidHTTPStatus{StatusCode: tc.code})
		var perm *backoff.PermanentError
		isPerm := errors.As(err, &perm)
		if isPerm != tc.permanent {
			t.Errorf("status %d: expected permanent=%v, got permanent=%v", tc.code, tc.permanent, isPerm)
		}
	}
}

func TestHTTPStatusPolicy_PermanentPreservesUnderlyingError(t *testing.T) {
	t.Parallel()
	original := kawa.ErrInvalidHTTPStatus{StatusCode: http.StatusNotFound}
	classified := kawa.NewHTTPStatusPolicy().Classify(original)
	if _, ok := errors.AsType[kawa.ErrInvalidHTTPStatus](classified); !ok {
		t.Errorf("expected ErrInvalidHTTPStatus to be unwrappable from Permanent, got: %T", classified)
	}
}

// ── NewExponentialRetryPolicy ─────────────────────────────────────────────────

func TestNewExponentialRetryPolicy_MaxRetries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxRetries uint64
	}{
		{"configured value is returned", 5},
		{"zero disables retry", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := kawa.NewExponentialRetryPolicy(tc.maxRetries, 0).MaxRetries(); got != tc.maxRetries {
				t.Errorf("MaxRetries: want %d, got %d", tc.maxRetries, got)
			}
		})
	}
}

func TestNewExponentialRetryPolicy_NewBackOffReturnsFreshInstance(t *testing.T) {
	t.Parallel()
	p := kawa.NewExponentialRetryPolicy(3, 0)
	if b1, b2 := p.NewBackOff(), p.NewBackOff(); b1 == b2 {
		t.Error("expected NewBackOff to return a new instance on each call")
	}
}

func TestNewExponentialRetryPolicy_InitialInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		interval     time.Duration
		wantInterval time.Duration
	}{
		{"zero uses library default (500ms)", 0, backoff.DefaultInitialInterval},
		{"custom interval is applied", 200 * time.Millisecond, 200 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eb, ok := kawa.NewExponentialRetryPolicy(3, tc.interval).NewBackOff().(*backoff.ExponentialBackOff)
			if !ok {
				t.Fatal("expected *backoff.ExponentialBackOff")
			}
			if eb.InitialInterval != tc.wantInterval {
				t.Errorf("InitialInterval: want %v, got %v", tc.wantInterval, eb.InitialInterval)
			}
		})
	}
}

// ── NoReq / NoRes marker methods ──────────────────────────────────────────────

func TestMarkerMethodsDoNotPanic(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		fn   func()
	}{
		{"NoReq.Req", func() { var n kawa.NoReq; n.Req() }},
		{"NoRes.Res", func() { var n kawa.NoRes; n.Res() }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn()
		})
	}
}

// ── ErrInvalidHTTPStatus.Error ────────────────────────────────────────────────

func TestErrInvalidHTTPStatusErrorMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  kawa.ErrInvalidHTTPStatus
	}{
		{
			name: "valid UTF-8 body produces valid JSON",
			err:  kawa.ErrInvalidHTTPStatus{StatusCode: 404, Status: "404 Not Found", Body: []byte(`{"detail":"not found"}`)},
		},
		{
			name: "invalid UTF-8 body does not panic",
			err:  kawa.ErrInvalidHTTPStatus{StatusCode: 500, Status: "500 Internal Server Error", Body: []byte{0xff, 0xfe}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := tc.err.Error()
			if len(msg) == 0 {
				t.Fatal("Error() returned empty string")
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(msg), &parsed); err != nil {
				t.Errorf("Error() did not produce valid JSON: %v — got: %s", err, msg)
			}
		})
	}
}
