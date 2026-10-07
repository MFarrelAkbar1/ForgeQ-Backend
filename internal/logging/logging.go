// Package logging builds the process-wide slog logger.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// New returns a logger writing to w. format is "json" or "text"; level is
// debug, info, warn or error. Unknown values fall back to json and info.
func New(w io.Writer, level, format string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}

	var h slog.Handler
	if strings.EqualFold(format, "text") {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return slog.New(h)
}
