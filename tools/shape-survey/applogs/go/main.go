// Go's two JSON loggers, side by side in one `net/http` app: `log/slog`'s JSONHandler at its
// defaults, and `zap`'s documented production configuration.
//
// Config source, copied rather than invented:
//
//	https://pkg.go.dev/log/slog#NewJSONHandler     -- `slog.NewJSONHandler(w, nil)` IS the default
//	                                                  configuration: `time`, `level`, `msg`, then
//	                                                  the call's own attributes.
//	https://pkg.go.dev/go.uber.org/zap#NewProduction
//	https://pkg.go.dev/go.uber.org/zap#NewProductionConfig
//
// zap is built through `zap.NewProductionConfig()` with **only `OutputPaths` changed**, from
// stderr to the file this survey tails. `NewProduction()` is documented as exactly that config
// built against stderr, so the encoder -- epoch-float `ts`, `level`, `caller`, `msg`, a
// `stacktrace` at error level and above -- is untouched, and it is the encoder that decides the
// record's shape.
//
// Neither library has a *second*, "bare default JSON" configuration to contrast with the way
// python-json-logger and pino do: slog's JSONHandler with nil options is already the default, and
// zap's only other JSON preset (`NewExample`) is documented as a testing convenience. The two
// rows here are therefore one default (slog) and one production (zap), labelled as such.
package main

import (
	"encoding/hex"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
)

// The shared route table (tools/shape-survey/applogs/python/appbase.py's `route`), so a width
// difference between two sources is the logging library and not a different application.
func route(path string) (int, string, bool) {
	clean := path
	if i := strings.IndexByte(clean, '?'); i >= 0 {
		clean = clean[:i]
	}
	segments := []string{}
	for _, s := range strings.Split(clean, "/") {
		if s != "" {
			segments = append(segments, s)
		}
	}
	if len(segments) == 0 {
		return 200, "ok\n", false
	}
	switch segments[0] {
	case "boom":
		return 500, "internal server error\n", true
	case "items":
		if len(segments) > 1 && segments[1] == "0" {
			return 404, "not found\n", false
		}
		return 200, "{\"items\":[]}\n", false
	case "search":
		return 200, "{\"results\":[]}\n", false
	case "healthz":
		return 200, "ok\n", false
	}
	return 404, "not found\n", false
}

func requestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func main() {
	slogPath, zapPath, port := os.Args[1], os.Args[2], os.Args[3]

	slogFile, err := os.OpenFile(slogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		panic(err)
	}
	logger := slog.New(slog.NewJSONHandler(slogFile, nil))

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{zapPath}
	cfg.ErrorOutputPaths = []string{zapPath}
	zapLogger, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	defer func() { _ = zapLogger.Sync() }()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		status, body, boom := route(r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))

		// The same eight access-log fields every app in this survey logs (appbase.FIELDS).
		method, path := r.Method, r.URL.RequestURI()
		duration := float64(time.Since(started).Microseconds()) / 1000.0
		size, remote := len(body), r.RemoteAddr
		agent := r.Header.Get("User-Agent")
		if agent == "" {
			agent = "-"
		}
		id := requestID()

		if boom {
			failure := fmt.Errorf("synthetic failure serving %s", path)
			logger.Error("request failed",
				"method", method, "path", path, "status", status,
				"duration_ms", duration, "bytes", size, "remote_addr", remote,
				"user_agent", agent, "request_id", id, "error", failure)
			zapLogger.Error("request failed",
				zap.String("method", method), zap.String("path", path), zap.Int("status", status),
				zap.Float64("duration_ms", duration), zap.Int("bytes", size),
				zap.String("remote_addr", remote), zap.String("user_agent", agent),
				zap.String("request_id", id), zap.Error(failure))
			return
		}
		logger.Info("request",
			"method", method, "path", path, "status", status,
			"duration_ms", duration, "bytes", size, "remote_addr", remote,
			"user_agent", agent, "request_id", id)
		zapLogger.Info("request",
			zap.String("method", method), zap.String("path", path), zap.Int("status", status),
			zap.Float64("duration_ms", duration), zap.Int("bytes", size),
			zap.String("remote_addr", remote), zap.String("user_agent", agent),
			zap.String("request_id", id))
	})

	logger.Info("starting", "port", port, "library", "log/slog")
	zapLogger.Info("starting", zap.String("port", port), zap.String("library", "zap"))
	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		panic(err)
	}
}
