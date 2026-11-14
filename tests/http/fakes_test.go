package tests

import (
	"io"
	"net/http"
)

// ── functional fakes ──────────────────────────────────────────────────────────
//
// Small composable types that satisfy io.Reader, io.Closer, and http.RoundTripper.
// Closures capture the exact failure state needed, keeping each test self-contained.

// roundTripFunc is a function that satisfies http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// readFunc is a function that satisfies io.Reader.
type readFunc func(p []byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) { return f(p) }

// readCloser composes a reader and a closer into an io.ReadCloser.
type readCloser struct {
	io.Reader
	closeErr error
}

func (rc readCloser) Close() error { return rc.closeErr }

// fakeResponse builds a minimal *http.Response with the given body and status.
func fakeResponse(status int, body io.ReadCloser) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       body,
		Header:     make(http.Header),
	}
}

// blockingTransport blocks until the request context is cancelled — no real
// network I/O, safe to use inside testing/synctest bubbles.
type blockingTransport struct{}

func (blockingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}
