package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/v8tix/rxgo/v2"

	"github.com/v8tix/kawa"
)

// ── NewCall / Do ──────────────────────────────────────────────────────────────

func TestNewCallDoReturnsResponseOnSuccess(t *testing.T) {
	t.Parallel()
	call := kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, getUserTS.URL)
	env, err := call.Do(context.Background(), nil)
	if err != nil {
		t.Fatalf("Do: unexpected error: %v", err)
	}
	if !cmp.Equal(userEnvRes, env.Body) {
		t.Error(cmp.Diff(userEnvRes, env.Body))
	}
}

func TestNewCallDoReturns404AsPermanentError(t *testing.T) {
	t.Parallel()
	_, err := kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, notFoundTS.URL).
		Do(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for 404 response, got nil")
	}
	if _, ok := errors.AsType[kawa.ErrInvalidHTTPStatus](err); !ok {
		t.Errorf("expected ErrInvalidHTTPStatus, got %T: %v", err, err)
	}
}

func TestNewCallDoWithCustomHeadersSendsHeaders(t *testing.T) {
	t.Parallel()

	wantAuth := "Bearer test-token"
	var gotAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	_, _ = kawa.NewCall[kawa.NoReq, kawa.NoRes](newTestHTTPClient(), kawa.Get, ts.URL).
		WithHeaders(map[string]string{"Authorization": wantAuth}).
		Do(context.Background(), nil)

	if gotAuth != wantAuth {
		t.Errorf("Authorization header: got %q, want %q", gotAuth, wantAuth)
	}
}

// ── NewCall / Observable ──────────────────────────────────────────────────────

func TestNewCallObservableReturnsResponseOnSuccess(t *testing.T) {
	t.Parallel()
	item := <-kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, getUserTS.URL).
		WithMaxRetries(0).
		Observable(context.Background(), nil).Observe()

	env, err := kawa.ItemValue[kawa.Envelope[getUserEnvV1Res]](item)
	if err != nil {
		t.Fatalf("Rx: unexpected error: %v", err)
	}
	if !cmp.Equal(userEnvRes, env.Body) {
		t.Error(cmp.Diff(userEnvRes, env.Body))
	}
}

func TestNewCallObservableRetriesTransientErrorsThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, envelope{"user": userRes})
	}))
	defer ts.Close()

	item := <-kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithRetryPolicy(zeroBackOffPolicy{n: 3}).
		Observable(context.Background(), nil).Observe()

	env, err := kawa.ItemValue[kawa.Envelope[getUserEnvV1Res]](item)
	if err != nil {
		t.Fatalf("Rx: unexpected error after retries: %v", err)
	}
	if !cmp.Equal(userEnvRes, env.Body) {
		t.Error(cmp.Diff(userEnvRes, env.Body))
	}
	if got := int(attempts.Load()); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

func TestNewCallObservableDoesNotRetryPermanent404(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	item := <-kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithRetryPolicy(zeroBackOffPolicy{n: 5}).
		Observable(context.Background(), nil).Observe()

	if _, err := kawa.ItemValue[kawa.Envelope[getUserEnvV1Res]](item); err == nil {
		t.Fatal("expected error for 404 response, got nil")
	}
	if got := int(attempts.Load()); got != 1 {
		t.Errorf("expected 1 attempt (permanent error), got %d", got)
	}
}

func TestNewCallObservableWith404MadeTransientDoesRetry(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	<-kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithRetryPolicy(zeroBackOffPolicy{n: 2}).
		WithErrorPolicy(kawa.NewHTTPStatusPolicy().WithTransient(http.StatusNotFound)).
		Observable(context.Background(), nil).Observe()

	if got := int(attempts.Load()); got != 3 {
		t.Errorf("expected 3 attempts when 404 is transient, got %d", got)
	}
}

// ── NewCall / DoWithRetry ──────────────────────────────────────────────────────

func TestNewCallDoWithRetryReturnsResponseOnSuccess(t *testing.T) {
	t.Parallel()
	env, err := kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, getUserTS.URL).
		WithMaxRetries(0).
		DoWithRetry(context.Background(), nil)
	if err != nil {
		t.Fatalf("DoWithRetry: unexpected error: %v", err)
	}
	if !cmp.Equal(userEnvRes, env.Body) {
		t.Error(cmp.Diff(userEnvRes, env.Body))
	}
}

func TestNewCallDoWithRetryRetriesTransientErrorsThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, envelope{"user": userRes})
	}))
	defer ts.Close()

	env, err := kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithRetryPolicy(zeroBackOffPolicy{n: 3}).
		DoWithRetry(context.Background(), nil)
	if err != nil {
		t.Fatalf("DoWithRetry: unexpected error after retries: %v", err)
	}
	if !cmp.Equal(userEnvRes, env.Body) {
		t.Error(cmp.Diff(userEnvRes, env.Body))
	}
	if got := int(attempts.Load()); got != 3 {
		t.Errorf("expected 3 attempts, got %d", got)
	}
}

func TestNewCallDoWithRetryDoesNotRetryPermanent404(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	_, err := kawa.NewCall[kawa.NoReq, getUserEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithRetryPolicy(zeroBackOffPolicy{n: 5}).
		DoWithRetry(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for 404 response, got nil")
	}
	if got := int(attempts.Load()); got != 1 {
		t.Errorf("expected 1 attempt (permanent error), got %d", got)
	}
}

// ── Deadline with testing/synctest ───────────────────────────────────────────

func TestNewCallDoDeadlineExceededReturnsErrTimeOut(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		client := &http.Client{Transport: blockingTransport{}}
		_, err := kawa.NewCall[kawa.NoReq, getUserEnvV1Res](client, kawa.Get, "http://fake.local/").
			WithDeadline(50*time.Millisecond).
			WithMaxRetries(0).
			Do(context.Background(), nil)

		if !errors.Is(err, kawa.ErrTimeOut) {
			t.Errorf("expected ErrTimeOut, got: %v", err)
		}
	})
}

// ── large response bodies ─────────────────────────────────────────────────────

// TestNewCallDoLargeResponseBodySucceeds reproduces a bug where sendRequest
// deferred cancel() on its own request context the moment it returned —
// before execute() had read the response body. Small bodies are already
// fully buffered by the time client.Do() returns, so the premature
// cancellation went unnoticed; a genuinely large body (like a full list
// endpoint) isn't, so the later read raced an already-canceled context and
// failed with "context canceled" — deterministically, not just under load.
func TestNewCallDoLargeResponseBodySucceeds(t *testing.T) {
	t.Parallel()
	env, err := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](newTestHTTPClient(), kawa.Get, manyUsersTS.URL).
		WithDeadline(5*time.Second).
		Do(context.Background(), nil)
	if err != nil {
		t.Fatalf("Do: unexpected error for large response body: %v", err)
	}
	if got := len(env.Body.Users); got != 5000 {
		t.Errorf("expected 5000 users, got %d", got)
	}
}

func TestNewCallDoWithRetryLargeResponseBodySucceeds(t *testing.T) {
	t.Parallel()
	env, err := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](newTestHTTPClient(), kawa.Get, manyUsersTS.URL).
		WithDeadline(5*time.Second).
		WithMaxRetries(0).
		DoWithRetry(context.Background(), nil)
	if err != nil {
		t.Fatalf("DoWithRetry: unexpected error for large response body: %v", err)
	}
	if got := len(env.Body.Users); got != 5000 {
		t.Errorf("expected 5000 users, got %d", got)
	}
}

// A deadline that fires while the body is still streaming (not just before
// headers arrive) must still translate to the friendly ErrTimeOut, not leak
// as a raw "context canceled" — proving the widened cancel scope (now
// covering the body read) still correctly distinguishes "our own timer
// fired" from any other cancellation source.
func TestNewCallDoDeadlineExceededDuringBodyReadReturnsErrTimeOut(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(slowStreamingBodyHandler))
	defer ts.Close()

	_, err := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithDeadline(50*time.Millisecond).
		Do(context.Background(), nil)
	if !errors.Is(err, kawa.ErrTimeOut) {
		t.Fatalf("expected ErrTimeOut, got: %v", err)
	}
}

// A caller-driven cancellation mid-body-read must still surface as the
// caller's own context error (not be mistaken for our own timer, and not be
// masked by the wider release scope now covering the read).
func TestNewCallDoCallerCancelDuringBodyReadReturnsContextDeadlineExceeded(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(slowStreamingBodyHandler))
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](newTestHTTPClient(), kawa.Get, ts.URL).
		WithDeadline(5*time.Second).
		Do(ctx, nil)
	if errors.Is(err, kawa.ErrTimeOut) {
		t.Fatalf("expected caller's own context error, not ErrTimeOut: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
}

// Retries must not leak release/timer state across attempts — a slow first
// attempt that times out, followed by a fast large-body second attempt,
// must succeed cleanly. Driven entirely by a fake transport inside a
// synctest bubble (no real server, no real sleep) so the outcome depends on
// the retry logic alone, never on wall-clock scheduling noise.
func TestNewCallDoWithRetryLargeResponseBodyAfterTransientTimeoutSucceeds(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		body, err := json.Marshal(envelope{"users": manyUsers(3000)})
		if err != nil {
			t.Fatalf("marshal fixture body: %v", err)
		}

		var attempts atomic.Int32
		client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if attempts.Add(1) == 1 {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return fakeResponse(http.StatusOK, io.NopCloser(bytes.NewReader(body))), nil
		})}

		env, err := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](client, kawa.Get, "http://fake.local/").
			WithDeadline(50*time.Millisecond).
			WithRetryPolicy(zeroBackOffPolicy{n: 1}).
			DoWithRetry(context.Background(), nil)
		if err != nil {
			t.Fatalf("unexpected error after retry: %v", err)
		}
		if got := len(env.Body.Users); got != 3000 {
			t.Errorf("expected 3000 users, got %d", got)
		}
		if got := int(attempts.Load()); got != 2 {
			t.Errorf("expected 2 attempts, got %d", got)
		}
	})
}

// Envelope.Response.Body must still be independently re-readable after Do
// for large payloads too, not just the small ones the original test covers.
func TestEnvelopeResponseBodyIsReadableAfterDoWithLargeBody(t *testing.T) {
	t.Parallel()
	env, err := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](newTestHTTPClient(), kawa.Get, manyUsersTS.URL).
		WithDeadline(5*time.Second).
		Do(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw, err := io.ReadAll(env.Response.Body)
	if err != nil {
		t.Fatalf("Envelope.Response.Body read failed: %v", err)
	}
	var reparsed manyUsersEnvV1Res
	if err := json.Unmarshal(raw, &reparsed); err != nil {
		t.Fatalf("raw body did not re-parse as valid JSON: %v", err)
	}
	if len(reparsed.Users) != 5000 {
		t.Errorf("expected 5000 users in re-read raw body, got %d", len(reparsed.Users))
	}
}

// Concurrent large-body calls must not race or leak resources under the new
// release mechanism.
func TestHTTPCallDoConcurrentCallsWithLargeBodiesDoNotRace(t *testing.T) {
	t.Parallel()
	call := kawa.NewCall[kawa.NoReq, manyUsersEnvV1Res](newTestHTTPClient(), kawa.Get, manyUsersTS.URL).
		WithDeadline(5 * time.Second)

	const goroutines = 10
	errc := make(chan error, goroutines)
	for range goroutines {
		go func() {
			env, err := call.Do(context.Background(), nil)
			if err == nil && len(env.Body.Users) != 5000 {
				err = fmt.Errorf("expected 5000 users, got %d", len(env.Body.Users))
			}
			errc <- err
		}()
	}
	for range goroutines {
		if err := <-errc; err != nil {
			t.Errorf("unexpected error in concurrent Do: %v", err)
		}
	}
}

// ── multi-method coverage ─────────────────────────────────────────────────────

func TestNewCallObservableAllMethodsWithNilBodySucceed(t *testing.T) {
	t.Parallel()
	for _, m := range []kawa.HTTPMethod{kawa.Get, kawa.Delete, kawa.Post, kawa.Put, kawa.Patch} {
		t.Run(string(m), func(t *testing.T) {
			t.Parallel()
			env, err := kawa.ItemValue[kawa.Envelope[getUserEnvV1Res]](<-kawa.NewCall[kawa.NoReq, getUserEnvV1Res](
				newTestHTTPClient(), m, getUserTS.URL,
			).WithDeadline(time.Second).WithRetryPolicy(zeroBackOffPolicy{n: 3}).
				Observable(context.Background(), nil).Observe())
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", m, err)
			}
			if !cmp.Equal(userEnvRes, env.Body) {
				t.Error(cmp.Diff(userEnvRes, env.Body))
			}
		})
	}
}

func TestNewCallObservableAllMethodsWithBodySucceed(t *testing.T) {
	t.Parallel()
	for _, m := range []kawa.HTTPMethod{kawa.Post, kawa.Put, kawa.Patch} {
		t.Run(string(m), func(t *testing.T) {
			t.Parallel()
			req := cuReq
			env, err := kawa.ItemValue[kawa.Envelope[getUserEnvV1Res]](<-kawa.NewCall[createUserV1Req, getUserEnvV1Res](
				newTestHTTPClient(), m, upsertUserTS.URL,
			).WithDeadline(time.Second).WithRetryPolicy(zeroBackOffPolicy{n: 3}).
				Observable(context.Background(), &req).Observe())
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", m, err)
			}
			if !cmp.Equal(userEnvRes, env.Body) {
				t.Error(cmp.Diff(userEnvRes, env.Body))
			}
		})
	}
}

func TestNewCallDoAllMethodsWithBodySucceed(t *testing.T) {
	t.Parallel()
	for _, m := range []kawa.HTTPMethod{kawa.Post, kawa.Put, kawa.Patch} {
		t.Run(string(m), func(t *testing.T) {
			t.Parallel()
			env, err := kawa.NewCall[createUserV1Req, getUserEnvV1Res](
				newTestHTTPClient(), m, upsertUserTS.URL,
			).WithDeadline(time.Second).Do(context.Background(), new(createUserV1Req))
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", m, err)
			}
			if !cmp.Equal(userEnvRes, env.Body) {
				t.Error(cmp.Diff(userEnvRes, env.Body))
			}
		})
	}
}

// ── concurrent safety ─────────────────────────────────────────────────────────

func TestHTTPCallDoConcurrentCallsDoNotRaceOnHeaders(t *testing.T) {
	t.Parallel()
	call := kawa.NewCall[createUserV1Req, getUserEnvV1Res](
		httpClient, kawa.Get, getUserTS.URL,
	).WithDeadline(time.Second).WithHeaders(map[string]string{"X-Concurrent": "test"})

	const goroutines = 10
	errc := make(chan error, goroutines)
	for range goroutines {
		go func() {
			_, err := call.Do(context.Background(), nil)
			errc <- err
		}()
	}
	for range goroutines {
		if err := <-errc; err != nil {
			t.Errorf("unexpected error in concurrent Do: %v", err)
		}
	}
}

// ── Envelope body readability ─────────────────────────────────────────────────

func TestEnvelopeResponseBodyIsReadableAfterDo(t *testing.T) {
	t.Parallel()
	env, err := kawa.NewCall[createUserV1Req, getUserEnvV1Res](httpClient, kawa.Post, upsertUserTS.URL).
		WithDeadline(time.Second).Do(context.Background(), new(createUserV1Req))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	raw, err := io.ReadAll(env.Response.Body)
	if err != nil {
		t.Fatalf("Envelope.Response.Body read failed: %v", err)
	}
	if len(raw) == 0 {
		t.Error("expected non-empty raw body, got empty")
	}
}

// ── WithClassifier / WithBodyLimit / WithErrorBodyLimit ───────────────────────

func TestWithClassifierIsUsedByObservable(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("custom classify error")
	items := kawa.NewCall[createUserV1Req, getUserEnvV1Res](httpClient, kawa.Get, notFoundTS.URL).
		WithDeadline(time.Second).
		WithRetryPolicy(zeroBackOffPolicy{n: 0}).
		WithClassifier(kawa.ErrorClassifier(func(err error) error { return sentinel })).
		Observable(context.Background(), nil).Observe()

	item, ok := <-items
	if !ok {
		t.Fatal("expected an item from observable")
	}
	if !errors.Is(item.E, sentinel) {
		t.Errorf("expected sentinel from classifier, got: %v", item.E)
	}
}

func TestWithBodyLimitTruncatesLargeSuccessBody(t *testing.T) {
	t.Parallel()
	_, err := kawa.NewCall[createUserV1Req, getUserEnvV1Res](httpClient, kawa.Get, largeBodyTS.URL).
		WithDeadline(time.Second).
		WithBodyLimit(1).
		Do(context.Background(), nil)
	if err == nil {
		t.Fatal("expected decode error due to truncated body, got nil")
	}
}

func TestWithErrorBodyLimitCapsErrorBody(t *testing.T) {
	t.Parallel()
	const limit = int64(5)
	_, err := kawa.NewCall[createUserV1Req, getUserEnvV1Res](httpClient, kawa.Get, badRequestBodyTS.URL).
		WithDeadline(time.Second).
		WithErrorBodyLimit(limit).
		Do(context.Background(), nil)

	var httpErr kawa.ErrInvalidHTTPStatus
	if !errors.As(err, &httpErr) {
		t.Fatalf("expected ErrInvalidHTTPStatus, got: %T %v", err, err)
	}
	if int64(len(httpErr.Body)) > limit {
		t.Errorf("body stored %d bytes, expected at most %d", len(httpErr.Body), limit)
	}
}

// ── ErrorClassifier ───────────────────────────────────────────────────────────

func TestErrorClassifierImplementsErrorPolicy(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("classified")
	clf := kawa.ErrorClassifier(func(err error) error { return sentinel })
	if got := clf.Classify(errors.New("input")); !errors.Is(got, sentinel) {
		t.Errorf("expected sentinel, got: %v", got)
	}
}

// ── ItemValue ─────────────────────────────────────────────────────────────────

func TestItemValueWithValidPointerReturnsValue(t *testing.T) {
	t.Parallel()
	v := userResV{ID: "abc"}
	if _, err := kawa.ItemValue[userResV](rxgo.Item{V: &v}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestItemValueWithNonPointerReturnsErrNonPointer(t *testing.T) {
	t.Parallel()
	v := userResV{ID: "abc"}
	_, err := kawa.ItemValue[userResV](rxgo.Item{V: v})
	if !errors.Is(err, kawa.ErrNonPointerOrWrongCasting) {
		t.Errorf("expected ErrNonPointerOrWrongCasting, got: %v", err)
	}
}

func TestItemValueWithErrorValueReturnsError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("item error")
	_, err := kawa.ItemValue[userResV](rxgo.Item{V: sentinel})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel, got: %v", err)
	}
}

func TestItemValueWithNilValueReturnsErrEmptyItem(t *testing.T) {
	t.Parallel()
	_, err := kawa.ItemValue[userResV](rxgo.Item{V: nil})
	if !errors.Is(err, kawa.ErrEmptyItem) {
		t.Errorf("expected ErrEmptyItem, got: %v", err)
	}
}

func TestItemValueWithNilValueAndErrorReturnsError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("operation error")
	_, err := kawa.ItemValue[userResV](rxgo.Item{V: nil, E: sentinel})
	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error, got: %v", err)
	}
}

func TestItemValueWithWrongTypeReturnsErrNonPointer(t *testing.T) {
	t.Parallel()
	// Item holds *userResV but we request createUserV1Req — type mismatch → ErrNonPointerOrWrongCasting.
	v := userResV{ID: "abc"}
	_, err := kawa.ItemValue[createUserV1Req](rxgo.Item{V: &v})
	if !errors.Is(err, kawa.ErrNonPointerOrWrongCasting) {
		t.Errorf("expected ErrNonPointerOrWrongCasting, got: %v", err)
	}
}

// ── execute I/O failure paths ─────────────────────────────────────────────────

func TestExecuteBodyIOFailures(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	readErr := errors.New("network read error")
	validBody := `{"user":{"name":"Marco","sms_number":"123","enabled":false}}`

	tests := []struct {
		name      string
		transport roundTripFunc
		wantErr   error
	}{
		{
			name: "body reads fine but Close() fails — close error surfaced",
			transport: func(r *http.Request) (*http.Response, error) {
				rec := httptest.NewRecorder()
				rec.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprint(rec, validBody)
				resp := rec.Result()
				resp.Body = readCloser{Reader: resp.Body, closeErr: closeErr}
				return resp, nil
			},
			wantErr: closeErr,
		},
		{
			name: "body read fails — read error returned",
			transport: func(r *http.Request) (*http.Response, error) {
				return fakeResponse(http.StatusOK, readCloser{
					Reader: readFunc(func(p []byte) (int, error) { return 0, readErr }),
				}), nil
			},
			wantErr: readErr,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &http.Client{Transport: tc.transport}
			_, err := kawa.NewCall[createUserV1Req, getUserEnvV1Res](client, kawa.Get, "http://fake").
				WithDeadline(time.Second).
				Do(context.Background(), nil)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("want %v, got %v", tc.wantErr, err)
			}
		})
	}
}
