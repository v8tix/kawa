package kawa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/reactivex/rxgo/v2"
	"github.com/v8tix/jsonx"
)

const (
	Get            = HTTPMethod("GET")
	Post           = HTTPMethod("POST")
	Put            = HTTPMethod("PUT")
	Patch          = HTTPMethod("PATCH")
	Delete         = HTTPMethod("DELETE")
	DefaultTimeout = 15 * time.Second

	// maxErrorBodyBytes caps the response body stored in ErrInvalidHTTPStatus.
	// Prevents a rogue server from exhausting memory on error responses.
	maxErrorBodyBytes = 64 * 1024 // 64 KB

	// maxResponseBodyBytes caps the success response body read in execute.
	maxResponseBodyBytes = 10 * 1024 * 1024 // 10 MB
)

var (
	ErrTimeOut                  = kawaErr("timeout occurred")
	ErrMarshalValue             = kawaErr("failed to marshal the value to JSON")
	ErrBadRequest               = kawaErr("bad request")
	ErrNonPointerOrWrongCasting = kawaErr("value is not a pointer or casting type is incorrect")
	ErrEmptyItem                = kawaErr("item has no value and no error")
	ErrNilValue                 = kawaErr("nil value provided")
)

type (
	Req interface{ Req() }
	Res interface{ Res() }

	ReqURLI interface {
		URLValues() interface{ Encode() string }
	}

	// NoReq is a sentinel request type for HTTP calls with no request body (e.g. GET).
	NoReq string
	// NoRes is a sentinel response type for HTTP calls with no response body (e.g. 204 No Content).
	NoRes string

	HTTPMethod string

	// Envelope wraps the decoded response body together with the underlying
	// *http.Response for status code and header access.
	//
	// Note: Response.Body has already been read and closed by the time the caller
	// receives Envelope. It is replaced with an io.NopCloser over the raw bytes so
	// that callers can still read the original response payload if needed.
	Envelope[T Res] struct {
		Body *T
		*http.Response
	}

	kawaErr string

	// ErrInvalidHTTPStatus is returned when the server replies with a 4xx or 5xx
	// status. The response body is eagerly read (up to maxErrorBodyBytes) and stored
	// so that Error() is idempotent and safe to call multiple times.
	ErrInvalidHTTPStatus struct {
		StatusCode int
		Status     string
		Body       []byte
	}
)

// Req satisfies Req. NoReq carries no data; the method exists solely to
// let the type system express "this call has no request body" without nil.
func (nrq NoReq) Req() { /* marker — no behavior required */ }

// Res satisfies Res. NoRes carries no data; the method exists solely to
// let the type system express "this call returns no response body" (e.g. 204 No Content).
func (nrp NoRes) Res()          { /* marker — no behavior required */ }
func (e kawaErr) Error() string { return string(e) }

// newErrInvalidHTTPStatus reads and closes res.Body (up to errorBodyLimit bytes) so
// the error value is self-contained and safe to inspect repeatedly.
// If the body cannot be fully read, the partial content is retained and a note
// about the read failure is appended so the caller is not silently misled.
func newErrInvalidHTTPStatus(res *http.Response, errorBodyLimit int64) ErrInvalidHTTPStatus {
	body, readErr := io.ReadAll(io.LimitReader(res.Body, errorBodyLimit))
	closeErr := res.Body.Close()

	switch {
	case readErr != nil && len(body) == 0:
		body = []byte(fmt.Sprintf("<error reading body: %s>", readErr))
	case readErr != nil:
		body = append(body, []byte(fmt.Sprintf(" <body truncated: %s>", readErr))...)
	case closeErr != nil:
		// Body was fully read; close failure doesn't affect the content.
	}

	return ErrInvalidHTTPStatus{
		StatusCode: res.StatusCode,
		Status:     res.Status,
		Body:       body,
	}
}

func (e ErrInvalidHTTPStatus) Error() string {
	b, _ := json.Marshal(struct {
		StatusCode int    `json:"status_code,omitempty"`
		Status     string `json:"status,omitempty"`
		ErrMessage string `json:"error_message,omitempty"`
	}{
		StatusCode: e.StatusCode,
		Status:     e.Status,
		ErrMessage: string(e.Body),
	})
	return string(b)
}

// NewHTTPClient creates a pre-configured http.Client.
func NewHTTPClient(
	timeout time.Duration,
	redirectPolicy func(req *http.Request, via []*http.Request) error,
	transport http.RoundTripper,
) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: redirectPolicy,
	}
}

// ItemValue extracts a typed value from an rxgo.Item.
func ItemValue[T any](item rxgo.Item) (*T, error) {
	switch item.V.(type) {
	case *T:
		return item.V.(*T), nil
	case nil:
		if item.E != nil {
			return nil, item.E
		}
		return nil, ErrEmptyItem
	case error:
		return nil, item.V.(error)
	default:
		return nil, ErrNonPointerOrWrongCasting
	}
}

// ── internal helpers ──────────────────────────────────────────────────────────

func defaultInvalidStatusCodeValidator(response *http.Response) bool {
	return response.StatusCode >= 400
}

// sanitizeURL returns scheme+host+path only, stripping query and fragment to
// avoid leaking auth tokens or other sensitive query parameters in error messages.
func sanitizeURL(u *url.URL) string {
	safe := *u
	safe.RawQuery = ""
	safe.Fragment = ""
	return safe.String()
}

func execute[T Req, U Res](
	ctx context.Context,
	client *http.Client,
	method HTTPMethod,
	endpoint string,
	request *T,
	cfg Config,
) (env *Envelope[U], err error) {
	reader, err := buildReader[T](request)
	if err != nil {
		return nil, err
	}

	// Avoid passing a typed nil (*bytes.Reader) as io.Reader — a non-nil interface
	// with a nil concrete value would panic inside http.NewRequestWithContext.
	var bodyReader io.Reader
	if reader != nil {
		bodyReader = reader
	}

	httpResponse, err := sendRequest(ctx, client, endpoint, cfg.Headers, bodyReader, cfg.Deadline, method, defaultInvalidStatusCodeValidator, cfg.ErrorBodyLimit)
	if err != nil {
		return nil, err
	}
	// Capture original body before we potentially replace it below, so the defer
	// always closes the underlying network connection — not a NopCloser over bytes.
	originalBody := httpResponse.Body
	defer func() {
		if closeErr := originalBody.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	if httpResponse.StatusCode == http.StatusNoContent {
		return newResponse[U](nil, httpResponse), nil
	}

	// Read the body into memory so we can:
	//   1. Decode it into the typed response.
	//   2. Restore it on Envelope.Response.Body for callers who need the raw bytes.
	rawBody, err := io.ReadAll(io.LimitReader(httpResponse.Body, cfg.BodyLimit))
	if err != nil {
		return nil, err
	}

	body, err := jsonx.ReadJSONAs[U](bytes.NewReader(rawBody))
	if err != nil {
		return nil, err
	}

	// Replace body with a re-readable NopCloser so Envelope.Response.Body works.
	httpResponse.Body = io.NopCloser(bytes.NewReader(rawBody))
	return newResponse[U](&body, httpResponse), nil
}

func newResponse[T Res](body *T, res *http.Response) *Envelope[T] {
	return &Envelope[T]{Body: body, Response: res}
}

func sendRequest(
	ctx context.Context,
	client *http.Client,
	rawURL string,
	headers map[string]string,
	body io.Reader,
	deadline time.Duration,
	method HTTPMethod,
	statusCodeValidator func(res *http.Response) bool,
	errorBodyLimit int64,
) (*http.Response, error) {
	withCancelCtx, cancel := context.WithCancel(ctx)
	defer cancel() // always release the child context; calling cancel twice is safe
	timer := time.AfterFunc(deadline, cancel)
	defer func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}()

	request, err := http.NewRequestWithContext(withCancelCtx, string(method), rawURL, body)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		request.Header.Add(k, v)
	}

	res, err := client.Do(request)
	if err != nil {
		// Distinguish our own timer-induced cancellation from a deliberate caller
		// cancellation. Our timer fires cancel() on withCancelCtx but leaves the
		// parent ctx untouched, so ctx.Err() == nil exactly when our timer fired.
		if urlErr, ok := errors.AsType[*url.Error](err); ok &&
			(errors.Is(urlErr.Err, context.Canceled) || errors.Is(urlErr.Err, context.DeadlineExceeded)) &&
			ctx.Err() == nil {
			return nil, fmt.Errorf("service at %q %w", sanitizeURL(request.URL), ErrTimeOut)
		}
		return nil, err
	}
	if statusCodeValidator(res) {
		return nil, newErrInvalidHTTPStatus(res, errorBodyLimit)
	}
	return res, nil
}

func marshalToReader[T any](value *T) (*bytes.Reader, error) {
	if value == nil {
		return nil, ErrNilValue
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", err.Error(), ErrMarshalValue)
	}
	return bytes.NewReader(data), nil
}

func buildReader[T Req](request *T) (*bytes.Reader, error) {
	if request == nil {
		return nil, nil
	}
	if reqURL, ok := any(request).(interface {
		URLValues() interface{ Encode() string }
	}); ok {
		return bytes.NewReader([]byte(reqURL.URLValues().Encode())), nil
	}
	return marshalToReader[T](request)
}
