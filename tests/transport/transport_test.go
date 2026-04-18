package tests

import (
	"testing"
	"time"

	"github.com/v8tix/kawa/transport"
)

func TestIdleConnectionTimeout_ReturnsTransportWithConfiguredTimeout(t *testing.T) {
	t.Parallel()
	tr := transport.IdleConnectionTimeout(30 * time.Second)
	if tr == nil {
		t.Fatal("expected non-nil transport")
	}
	if tr.IdleConnTimeout != 30*time.Second {
		t.Errorf("IdleConnTimeout: want 30s, got %v", tr.IdleConnTimeout)
	}
}

func TestIdleConnectionTimeout_ZeroDurationIsAllowed(t *testing.T) {
	t.Parallel()
	tr := transport.IdleConnectionTimeout(0)
	if tr == nil {
		t.Fatal("expected non-nil transport")
	}
	if tr.IdleConnTimeout != 0 {
		t.Errorf("IdleConnTimeout: want 0, got %v", tr.IdleConnTimeout)
	}
}

func TestIdleConnectionTimeout_EachCallReturnsNewInstance(t *testing.T) {
	t.Parallel()
	tr1 := transport.IdleConnectionTimeout(5 * time.Second)
	tr2 := transport.IdleConnectionTimeout(5 * time.Second)
	if tr1 == tr2 {
		t.Error("expected each call to return a new transport instance")
	}
}
