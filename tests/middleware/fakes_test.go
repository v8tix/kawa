package tests

import (
	"net/http"
	"net/http/httptest"
)

// roundTripFunc is a function that satisfies http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// okTransport returns a RoundTripper that always responds 200 OK.
func okTransport() http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		rec := httptest.NewRecorder()
		rec.WriteHeader(http.StatusOK)
		return rec.Result(), nil
	})
}

// errTransport returns a RoundTripper that always returns the given error.
func errTransport(err error) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, err
	})
}
