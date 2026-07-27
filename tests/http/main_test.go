package tests

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/v8tix/kawa"
	"github.com/v8tix/kawa/policy"
	"github.com/v8tix/kawa/transport"
)

// ── domain types ──────────────────────────────────────────────────────────────

type (
	createUserV1Req struct {
		Name  string `json:"name,omitempty"`
		Email string `json:"email,omitempty"`
	}

	getUserV1Res struct {
		Name      string `json:"name,omitempty"`
		SmsNumber string `json:"sms_number,omitempty"`
		Enabled   bool   `json:"enabled,omitempty"`
	}

	getUserEnvV1Res struct {
		User getUserV1Res `json:"user"`
	}

	userResV struct {
		ID string `json:"id,omitempty"`
	}

	// manyUsersEnvV1Res mirrors a "list" endpoint response shape (e.g. a full
	// order history) — large and growing, unlike the single-object responses
	// the other fixtures model.
	manyUsersEnvV1Res struct {
		Users []getUserV1Res `json:"users"`
	}
)

func (createUserV1Req) Req()   {}
func (getUserEnvV1Res) Res()   {}
func (userResV) Res()          {}
func (manyUsersEnvV1Res) Res() {}

// ── shared fixtures ───────────────────────────────────────────────────────────

var (
	httpClient = func() *http.Client { return newTestHTTPClient() }()

	userRes    = getUserV1Res{Name: "Marco", SmsNumber: "1800-some-number"}
	userEnvRes = &getUserEnvV1Res{User: userRes}
	cuReq      = createUserV1Req{Name: "Adam Smith", Email: "adam.smith@hotmail.com"}

	// Shared test servers — started by TestMain, closed after m.Run().
	getUserTS        *httptest.Server
	upsertUserTS     *httptest.Server
	slowUserTS       *httptest.Server
	notFoundTS       *httptest.Server
	largeBodyTS      *httptest.Server
	badRequestBodyTS *httptest.Server
	manyUsersTS      *httptest.Server
)

// ── TestMain ──────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	getUserTS = httptest.NewServer(http.HandlerFunc(getUserHandler))
	upsertUserTS = httptest.NewServer(http.HandlerFunc(upsertUserHandler))
	slowUserTS = httptest.NewServer(http.HandlerFunc(slowUpsertUserHandler))
	notFoundTS = httptest.NewServer(http.HandlerFunc(notFoundHandler))
	largeBodyTS = httptest.NewServer(http.HandlerFunc(largeBodyHandler))
	badRequestBodyTS = httptest.NewServer(http.HandlerFunc(badRequestBodyHandler))
	manyUsersTS = httptest.NewServer(http.HandlerFunc(manyUsersHandler))

	code := m.Run()

	getUserTS.Close()
	upsertUserTS.Close()
	slowUserTS.Close()
	notFoundTS.Close()
	largeBodyTS.Close()
	badRequestBodyTS.Close()
	manyUsersTS.Close()

	os.Exit(code)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newTestHTTPClient() *http.Client {
	return kawa.NewHTTPClient(5*time.Second, policy.OneRedirect, transport.IdleConnectionTimeout(kawa.DefaultTimeout))
}

// zeroBackOffPolicy makes retries happen instantly — safe for unit tests.
type zeroBackOffPolicy struct{ n uint64 }

func (p zeroBackOffPolicy) MaxRetries() uint64          { return p.n }
func (p zeroBackOffPolicy) NewBackOff() backoff.BackOff { return &backoff.ZeroBackOff{} }

// ── handlers ──────────────────────────────────────────────────────────────────

type envelope map[string]any

func getUserHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, envelope{"user": userRes})
}

func upsertUserHandler(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1_048_576)
	var req createUserV1Req
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, envelope{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, envelope{"user": userRes})
}

func slowUpsertUserHandler(w http.ResponseWriter, r *http.Request) {
	time.Sleep(500 * time.Millisecond)
	upsertUserHandler(w, r)
}

func notFoundHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotFound, envelope{"error": errors.New("user not found").Error()})
}

func largeBodyHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"user":{"name":"Marco","sms_number":"123","enabled":false}}`)
}

func badRequestBodyHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusBadRequest)
	fmt.Fprint(w, `{"message":"this is a long error message that exceeds the limit"}`)
}

// manyUsersHandler writes a genuinely large response (unlike largeBodyTS,
// which despite its name writes a tiny body). Big enough that it can't be
// fully received in a single read before the client gets control back,
// reproducing the same call shape as a real "list" endpoint.
func manyUsersHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, envelope{"users": manyUsers(5000)})
}

func manyUsers(n int) []getUserV1Res {
	users := make([]getUserV1Res, n)
	for i := range users {
		users[i] = getUserV1Res{
			Name:      fmt.Sprintf("User Number %d With A Reasonably Long Name", i),
			SmsNumber: fmt.Sprintf("+1-800-555-%04d", i%10000),
			Enabled:   i%2 == 0,
		}
	}
	return users
}

// slowStreamingBodyHandler flushes headers and a partial body immediately,
// then sleeps before finishing the write — so the body is still open and
// mid-stream when a short deadline should fire, not already fully received.
func slowStreamingBodyHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"users":[`)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	time.Sleep(200 * time.Millisecond)
	fmt.Fprint(w, `]}`)
}

func writeJSON(w http.ResponseWriter, status int, data envelope) {
	js, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(js)
}
