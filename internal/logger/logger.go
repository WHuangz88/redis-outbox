package logger

import (
	"context"
	"log/slog"
	"os"

	"kafka-demo/internal/domain"
)

// SlogLogger is a structured logger wrapper around log/slog.
type SlogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a new structured JSON logger.
func NewSlogLogger() *SlogLogger {
	return &SlogLogger{
		logger: slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	}
}

// Info prints structured info logs, automatically retrieving correlation_id from context.
func (l *SlogLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.InfoContext(ctx, msg, l.enrich(ctx, keysAndValues...)...)
}

// Error prints structured error logs, automatically retrieving correlation_id and attaching the error value.
func (l *SlogLogger) Error(ctx context.Context, msg string, err error, keysAndValues ...interface{}) {
	args := keysAndValues
	if err != nil {
		args = append(args, "error", err.Error())
	}
	l.logger.ErrorContext(ctx, msg, l.enrich(ctx, args...)...)
}

// enrich appends context correlation IDs (request_id) to the logging arguments.
func (l *SlogLogger) enrich(ctx context.Context, args ...interface{}) []interface{} {
	cid := domain.GetCorrelationID(ctx)
	if cid == "" {
		return args
	}
	// Prefix with request_id/correlation_id for system traceability
	res := make([]interface{}, 0, len(args)+2)
	res = append(res, "request_id", cid)
	res = append(res, args...)
	return res
}
