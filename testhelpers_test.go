package kawa

// Shared fixtures and TestMain for white-box tests (package kawa).
// Shared servers are only those needed by tests in this file.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/v8tix/jsonx"
	"github.com/v8tix/kawa/middleware"
	"github.com/v8tix/kawa/policy"
	"github.com/v8tix/kawa/transport"
)

// ── domain types ──────────────────────────────────────────────────────────────

type (
	nonSerdeType string

	envelope map[string]any

	createUserV1Req struct {
		Name  string `json:"name,omitempty"`
		Email string `json:"email,omitempty"`
	}

	getUserV1Res struct {
		Name      string `json:"name,omitempty"`
		SmsNumber string `json:"sms_number,omitempty"`
		Enabled   bool   `json:"enabled,omitempty"`
	}
)

func (n *nonSerdeType) UnmarshalJSON(_ []byte) error { return errors.New("non-serde struct") }
func (n nonSerdeType) MarshalJSON() ([]byte, error)  { return nil, errors.New("non-serde struct") }

func (createUserV1Req) Req() {}

// ── shared fixtures ───────────────────────────────────────────────────────────

var (
	nonSerde    = nonSerdeType("")
	badlyFormed = func() io.Reader { return strings.NewReader(`{ "name" : "Marco"`) }()

	linkHeader = map[string]string{
		"Link": `<https://api.github.com/repositories/1300192/issues?page=2>; rel="prev", ` +
			`<https://api.github.com/repositories/1300192/issues?page=4>; rel="next", ` +
			`<https://api.github.com/repositories/1300192/issues?page=515>; rel="last", ` +
			`<https://api.github.com/repositories/1300192/issues?page=1>; rel="first"`,
	}
	authorizationHeader = map[string]string{
		"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" +
			".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ" +
			".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c",
	}
	headers = lo.Assign[string, string](linkHeader, authorizationHeader)

	httpClient = func() *http.Client { return newTestHTTPClient() }()

	customHeadersHTTPClient = func(h middleware.CustomHeaders) *http.Client {
		return NewHTTPClient(5*time.Second, policy.OneRedirect, h)
	}

	userRes     = getUserV1Res{Name: "Marco", SmsNumber: "1800-some-number"}
	cuReq       = createUserV1Req{Name: "Adam Smith", Email: "adam.smith@hotmail.com"}
	cuReqReader = func() *bytes.Reader {
		b, _ := json.Marshal(cuReq)
		return bytes.NewReader(b)
	}

	// Shared test servers — started by TestMain, closed after m.Run().
	slowUserTS   *httptest.Server
	headerEchoTS *httptest.Server
)

// ── TestMain ──────────────────────────────────────────────────────────────────

func TestMain(m *testing.M) {
	slowUserTS = httptest.NewServer(http.HandlerFunc(slowUpsertUserHandler))
	headerEchoTS = httptest.NewServer(http.HandlerFunc(headerEchoHandler))

	code := m.Run()

	slowUserTS.Close()
	headerEchoTS.Close()

	os.Exit(code)
}

// ── constructors ──────────────────────────────────────────────────────────────

func newTestHTTPClient() *http.Client {
	return NewHTTPClient(5*time.Second, policy.OneRedirect, transport.IdleConnectionTimeout(DefaultTimeout))
}

// ── handlers ──────────────────────────────────────────────────────────────────

func headerEchoHandler(w http.ResponseWriter, r *http.Request) {
	for k, v := range r.Header {
		w.Header().Set(k, v[0])
	}
	if err := write(w, http.StatusOK, envelope{"user": userRes}, nil); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func slowUpsertUserHandler(w http.ResponseWriter, r *http.Request) {
	time.Sleep(500 * time.Millisecond)
	r.Body = http.MaxBytesReader(w, r.Body, 1_048_576)
	if _, err := jsonx.ReadJSONAs[createUserV1Req](r.Body); err != nil {
		errorResponse(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := write(w, http.StatusOK, envelope{"user": userRes}, nil); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func errorResponse(w http.ResponseWriter, _ *http.Request, status int, message any) {
	if err := write(w, status, envelope{"error": message}, nil); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func write(w http.ResponseWriter, status int, data envelope, extraHeaders http.Header) error {
	js, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return err
	}
	js = append(js, '\n')
	maps.Copy(w.Header(), extraHeaders)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(js)
	return nil
}

// ── request helpers ───────────────────────────────────────────────────────────

func unmarshalReq[T Req](req *http.Request) (*T, error) {
	if req == nil {
		return nil, ErrNilValue
	}
	genReq, err := jsonx.ReadJSONAs[T](req.Body)
	if err != nil {
		return nil, fmt.Errorf("%q: %w", err.Error(), ErrBadRequest)
	}
	return &genReq, nil
}
