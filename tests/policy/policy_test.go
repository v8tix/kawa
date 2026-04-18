package tests

import (
	"net/http"
	"testing"

	"github.com/v8tix/kawa/policy"
)

func TestOneRedirect_AllowsFirstRedirect(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	via := []*http.Request{req} // one redirect already followed
	if err := policy.OneRedirect(req, via); err != nil {
		t.Errorf("expected first redirect to be allowed, got: %v", err)
	}
}

func TestOneRedirect_BlocksSecondRedirect(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	via := []*http.Request{req, req} // two redirects already followed
	if err := policy.OneRedirect(req, via); err == nil {
		t.Error("expected second redirect to be blocked, got nil")
	}
}

func TestOneRedirect_AllowsNoRedirect(t *testing.T) {
	t.Parallel()
	req, _ := http.NewRequest(http.MethodGet, "http://example.com", nil)
	if err := policy.OneRedirect(req, nil); err != nil {
		t.Errorf("expected no redirect to be allowed, got: %v", err)
	}
}
