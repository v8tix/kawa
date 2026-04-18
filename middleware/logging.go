package middleware

import (
	"context"
	"log/slog"
	"net/http"
)

// LoggingTransport is an http.RoundTripper that logs each request and response
// using a structured slog.Logger. It wraps a next RoundTripper so it can be
// composed with other middleware (e.g. CustomHeaders, IdleConnectionTimeout).
//
// Construct with NewLoggingTransport, then chain With* methods to customize
// the log level:
//
// inner := transport.IdleConnectionTimeout(kawa.DefaultTimeout)
// t := middleware.NewLoggingTransport(logger, inner).
//
//	WithLevel(slog.LevelDebug).
//	WithErrorLevel(slog.LevelError)
type LoggingTransport struct {
	log        *slog.Logger
	next       http.RoundTripper
	level      slog.Level // used for request dispatched + response received
	errorLevel slog.Level // used when the round-trip fails
}

// NewLoggingTransport creates a LoggingTransport that wraps next.
// If logger is nil, slog.Default() is used.
// If next is nil, http.DefaultTransport is used.
// Default levels: request/response → Info, error → Error.
func NewLoggingTransport(logger *slog.Logger, next http.RoundTripper) *LoggingTransport {
	if logger == nil {
		logger = slog.Default()
	}
	if next == nil {
		next = http.DefaultTransport
	}
	return &LoggingTransport{
		log:        logger,
		next:       next,
		level:      slog.LevelInfo,
		errorLevel: slog.LevelError,
	}
}

// WithLevel sets the log level used for both the outgoing request and the
// incoming response.
func (l *LoggingTransport) WithLevel(lvl slog.Level) *LoggingTransport {
	l.level = lvl
	return l
}

// WithErrorLevel sets the log level used when a request fails.
func (l *LoggingTransport) WithErrorLevel(lvl slog.Level) *LoggingTransport {
	l.errorLevel = lvl
	return l
}

func (l *LoggingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	l.log.Log(r.Context(), l.level, "sending request",
		slog.String("method", r.Method),
		slog.String("url", r.URL.String()),
		slog.String("proto", r.Proto),
	)
	resp, err := l.next.RoundTrip(r)
	if err != nil {
		l.log.Log(context.Background(), l.errorLevel, "request failed",
			slog.String("error", err.Error()),
		)
		return nil, err
	}
	l.log.Log(r.Context(), l.level, "response received",
		slog.String("proto", resp.Proto),
		slog.Int("status", resp.StatusCode),
	)
	return resp, nil
}
