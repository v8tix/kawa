package middleware

import "net/http"

// CustomHeaders is an http.RoundTripper middleware that injects a fixed set of
// headers into every outgoing request, then delegates to next.
type CustomHeaders struct {
	next    http.RoundTripper
	headers map[string]string
}

// NewCustomHeaders creates a CustomHeaders middleware that wraps next.
// If next is nil, http.DefaultTransport is used.
func NewCustomHeaders(headers map[string]string, next http.RoundTripper) CustomHeaders {
	if next == nil {
		next = http.DefaultTransport
	}
	return CustomHeaders{next: next, headers: headers}
}

func (c CustomHeaders) RoundTrip(r *http.Request) (*http.Response, error) {
	reqCopy := r.Clone(r.Context())
	for k, v := range c.headers {
		reqCopy.Header.Add(k, v)
	}
	return c.next.RoundTrip(reqCopy)
}
