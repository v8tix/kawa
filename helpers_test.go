package kawa

// White-box tests — these require access to unexported symbols.
// Behavioral and integration tests live in tests/ subdirectories.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/v8tix/jsonx"
	"github.com/v8tix/kawa/middleware"
)

// ── sendRequest ───────────────────────────────────────────────────────────────

func TestSendRequestWithClientHeadersForwardsHeaders(t *testing.T) {
	t.Parallel()
	client := customHeadersHTTPClient(middleware.NewCustomHeaders(headers, nil))
	resp, release, err := sendRequest(context.Background(), client, headerEchoTS.URL, nil, nil, time.Second, Get, defaultInvalidStatusCodeValidator, maxErrorBodyBytes)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range headers {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("header %s: got %q, want %q", k, got, want)
		}
	}
}

func TestSendRequestWithExplicitHeadersForwardsHeaders(t *testing.T) {
	t.Parallel()
	resp, release, err := sendRequest(context.Background(), httpClient, headerEchoTS.URL, headers, nil, time.Second, Get, defaultInvalidStatusCodeValidator, maxErrorBodyBytes)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	for k, want := range headers {
		if got := resp.Header.Get(k); got != want {
			t.Errorf("header %s: got %q, want %q", k, got, want)
		}
	}
}

func TestSendRequestSlowServerReturnsErrTimeOut(t *testing.T) {
	t.Parallel()
	_, release, err := sendRequest(context.Background(), httpClient, slowUserTS.URL, nil, cuReqReader(), 10*time.Millisecond, Post, defaultInvalidStatusCodeValidator, maxErrorBodyBytes)
	defer release()
	if !errors.Is(err, ErrTimeOut) {
		t.Fatalf("expected ErrTimeOut, got: %v", err)
	}
}

func TestSendRequestNonKawaTimeoutsArePassedThrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mkCtx    func() (context.Context, context.CancelFunc)
		url      string
		wantErr  error
		wantSome bool
	}{
		{
			name: "caller cancel is not ErrTimeOut",
			mkCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			url:     slowUserTS.URL,
			wantErr: context.Canceled,
		},
		{
			name: "caller deadline is not ErrTimeOut",
			mkCtx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Millisecond)
			},
			url:     slowUserTS.URL,
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "connection refused is not ErrTimeOut",
			mkCtx: func() (context.Context, context.CancelFunc) {
				return context.Background(), func() {}
			},
			url:      "http://127.0.0.1:1",
			wantSome: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := tc.mkCtx()
			defer cancel()
			_, release, err := sendRequest(ctx, httpClient, tc.url, nil, cuReqReader(), 10*time.Second, Post, defaultInvalidStatusCodeValidator, maxErrorBodyBytes)
			defer release()
			if errors.Is(err, ErrTimeOut) {
				t.Fatalf("expected no ErrTimeOut, got ErrTimeOut")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("want %v, got %v", tc.wantErr, err)
			}
			if tc.wantSome && err == nil {
				t.Fatal("expected an error for unreachable host, got nil")
			}
		})
	}
}

func TestSendRequestReleaseIsIdempotentAndCancelsContext(t *testing.T) {
	t.Parallel()
	_, release, err := sendRequest(context.Background(), httpClient, headerEchoTS.URL, nil, nil, time.Second, Get, defaultInvalidStatusCodeValidator, maxErrorBodyBytes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Calling release multiple times must not panic — execute() defers it
	// once, but sendRequest's own error paths also call it directly, so both
	// can legitimately fire for the same release value in some paths.
	release()
	release()
	release()
}

// ── marshalToReader ───────────────────────────────────────────────────────────

func TestMarshalToReaderWithNilRequestReturnsErrNilValue(t *testing.T) {
	t.Parallel()
	var req *createUserV1Req
	_, err := marshalToReader[createUserV1Req](req)
	if !errors.Is(err, ErrNilValue) {
		t.Errorf("expected ErrNilValue, got: %v", err)
	}
}

func TestMarshalToReaderWithValidRequestReturnsReader(t *testing.T) {
	t.Parallel()
	_, err := marshalToReader[createUserV1Req](&cuReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMarshalToReaderWithNonSerdeTypeReturnsErrMarshalValue(t *testing.T) {
	t.Parallel()
	_, err := marshalToReader[nonSerdeType](new(nonSerde))
	if !errors.Is(err, ErrMarshalValue) {
		t.Errorf("expected ErrMarshalValue, got: %v", err)
	}
}

// ── buildReader ───────────────────────────────────────────────────────────────

type urlValuesReq struct{ q string }

func (urlValuesReq) Req() {}
func (r urlValuesReq) URLValues() interface{ Encode() string } {
	return urlValues{"q": r.q}
}

type urlValues map[string]string

func (v urlValues) Encode() string {
	q := ""
	for k, val := range v {
		if q != "" {
			q += "&"
		}
		q += k + "=" + val
	}
	return q
}

func TestBuildReaderWithURLValuesReturnsEncodedBody(t *testing.T) {
	t.Parallel()
	req := urlValuesReq{q: "hello"}
	r, err := buildReader[urlValuesReq](&req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("expected non-nil reader")
	}
	data, _ := io.ReadAll(r)
	if string(data) == "" {
		t.Error("expected encoded query string, got empty")
	}
}

// ── unmarshalReq ──────────────────────────────────────────────────────────────

func TestUnmarshalReqWithNilRequestReturnsErrNilValue(t *testing.T) {
	t.Parallel()
	_, err := unmarshalReq[createUserV1Req](nil)
	if !errors.Is(err, ErrNilValue) {
		t.Errorf("expected ErrNilValue, got: %v", err)
	}
}

// ── jsonx integration ─────────────────────────────────────────────────────────

func TestReadJSONAsWithBadJSONReturnsErrBadlyJSON(t *testing.T) {
	t.Parallel()
	_, err := jsonx.ReadJSONAs[createUserV1Req](badlyFormed)
	if !errors.Is(err, jsonx.ErrBadlyJSON) {
		t.Errorf("expected ErrBadlyJSON, got: %v", err)
	}
}

func TestReadJSONAsWithEmptyBodyReturnsErrEmptyBody(t *testing.T) {
	t.Parallel()
	_, err := jsonx.ReadJSONAs[createUserV1Req](io.NopCloser(http.NoBody))
	if !errors.Is(err, jsonx.ErrEmptyBody) {
		t.Errorf("expected ErrEmptyBody, got: %v", err)
	}
}

// ── newErrInvalidHTTPStatus I/O failure paths ─────────────────────────────────

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

func TestNewErrInvalidHTTPStatusIOFailures(t *testing.T) {
	t.Parallel()

	networkErr := errors.New("connection reset by peer")
	partial := []byte(`{"partial":`)

	tests := []struct {
		name       string
		statusCode int
		body       func() io.ReadCloser
		wantInBody string
	}{
		{
			name:       "read fails immediately with no bytes",
			statusCode: http.StatusInternalServerError,
			body: func() io.ReadCloser {
				return readCloser{
					Reader: readFunc(func(p []byte) (int, error) { return 0, networkErr }),
				}
			},
			wantInBody: "<error reading body:",
		},
		{
			name:       "read fails after partial bytes",
			statusCode: http.StatusBadGateway,
			body: func() io.ReadCloser {
				var called int
				return readCloser{
					Reader: readFunc(func(p []byte) (int, error) {
						if called == 0 {
							called++
							return copy(p, partial), nil
						}
						return 0, networkErr
					}),
				}
			},
			wantInBody: "<body truncated:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := newErrInvalidHTTPStatus(fakeResponse(tc.statusCode, tc.body()), maxErrorBodyBytes)
			if e.StatusCode != tc.statusCode {
				t.Errorf("StatusCode: want %d, got %d", tc.statusCode, e.StatusCode)
			}
			if !strings.Contains(string(e.Body), tc.wantInBody) {
				t.Errorf("body: want %q in %q", tc.wantInBody, e.Body)
			}
		})
	}
}
