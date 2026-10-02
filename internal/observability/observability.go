package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type contextKey string

const correlationKey contextKey = "correlation_id"

func WithCorrelationID(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, correlationKey, value)
}

func CorrelationID(ctx context.Context) string {
	value, _ := ctx.Value(correlationKey).(string)
	return value
}

type Logger struct {
	std *log.Logger
}

func NewLogger() *Logger {
	return &Logger{std: log.Default()}
}

func (l *Logger) Log(ctx context.Context, level, message string, fields map[string]string) {
	entry := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level,
		"message":   message,
	}
	if correlationID := CorrelationID(ctx); correlationID != "" {
		entry["correlationId"] = correlationID
	}
	for key, value := range fields {
		if safeField(key) {
			entry[key] = value
		}
	}

	payload, err := json.Marshal(entry)
	if err == nil {
		l.std.Print(string(payload))
	}
}

func safeField(key string) bool {
	switch key {
	case "correlationId", "messageId", "transactionId", "walletId", "providerId",
		"method", "route", "status", "duration", "errorClass",
		"eventId", "aggregateId", "attempt", "owner", "lease", "lag",
		"result", "operation", "groupId", "dependency", "reason",
		"difference", "currency", "checkedEntries", "replay", "failureCode":
		return true
	default:
		return false
	}
}

func (l *Logger) Info(ctx context.Context, message string, fields map[string]string) {
	l.Log(ctx, "info", message, fields)
}

func (l *Logger) Error(ctx context.Context, message string, err error, fields map[string]string) {
	if fields == nil {
		fields = map[string]string{}
	}
	fields["errorClass"] = ErrorClass(err)
	l.Log(ctx, "error", message, fields)
}

// ErrorClass names the cause of an error without logging its message, which
// may contain hosts, user names or values: a timeout or cancellation, the
// PostgreSQL SQLSTATE, a connection failure, an AWS error code, a network error,
// or else the type of the innermost wrapped error.
func ErrorClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return "postgres_" + pgErr.Code
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) {
		return "postgres_connect"
	}
	var apiErr interface{ ErrorCode() string }
	if errors.As(err, &apiErr) {
		return "aws_" + apiErr.ErrorCode()
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return "network"
	}
	for next := errors.Unwrap(err); next != nil; next = errors.Unwrap(err) {
		err = next
	}
	return fmt.Sprintf("%T", err)
}

// Metrics is a small in-process registry exposed in the Prometheus text format.
// Label values must come from fixed sets (status, failure code, SQLSTATE), never
// from identifiers, so the number of series stays bounded.
type Metrics struct {
	mu       sync.RWMutex
	counters map[string]uint64
	gauges   map[string]float64
	hist     map[string]histogram
}

type histogram struct {
	count uint64
	sum   float64
}

func NewMetrics() *Metrics {
	return &Metrics{counters: make(map[string]uint64), gauges: make(map[string]float64), hist: make(map[string]histogram)}
}

func (m *Metrics) Inc(name string) {
	m.mu.Lock()
	m.counters[name]++
	m.mu.Unlock()
}

// IncLabeled increments one series of a counter family, e.g.
// wager_results_total{status="PROCESSED"}.
func (m *Metrics) IncLabeled(name string, labels map[string]string) {
	m.Inc(seriesKey(name, labels))
}

// SetGauge records the current value of a gauge.
func (m *Metrics) SetGauge(name string, value float64) {
	m.mu.Lock()
	m.gauges[name] = value
	m.mu.Unlock()
}

func (m *Metrics) Observe(name string, value time.Duration) {
	m.mu.Lock()
	current := m.hist[name]
	current.count++
	current.sum += value.Seconds()
	m.hist[name] = current
	m.mu.Unlock()
}

func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		m.mu.RLock()
		defer m.mu.RUnlock()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		families := make(map[string][]string)
		for _, series := range sortedKeys(m.counters) {
			families[familyName(series)] = append(families[familyName(series)], series)
		}
		for _, family := range sortedKeys(families) {
			_, _ = fmt.Fprintf(w, "# TYPE %s counter\n", family)
			for _, series := range families[family] {
				_, _ = fmt.Fprintf(w, "%s %d\n", series, m.counters[series])
			}
		}
		for _, name := range sortedKeys(m.gauges) {
			_, _ = fmt.Fprintf(w, "# TYPE %s gauge\n%s %g\n", name, name, m.gauges[name])
		}
		for _, name := range sortedKeys(m.hist) {
			value := m.hist[name]
			_, _ = fmt.Fprintf(w, "# TYPE %s_seconds summary\n%s_seconds_count %d\n%s_seconds_sum %f\n", name, name, value.count, name, value.sum)
		}
	})
}

// Snapshot returns the value of an unlabeled counter.
func (m *Metrics) Snapshot(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counters[name]
}

// SnapshotLabeled returns the value of one series of a labeled counter.
func (m *Metrics) SnapshotLabeled(name string, labels map[string]string) uint64 {
	return m.Snapshot(seriesKey(name, labels))
}

// Gauge returns the current value of a gauge.
func (m *Metrics) Gauge(name string) float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.gauges[name]
}

func seriesKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%q", key, labels[key]))
	}
	return name + "{" + strings.Join(pairs, ",") + "}"
}

func familyName(series string) string {
	if index := strings.IndexByte(series, '{'); index >= 0 {
		return series[:index]
	}
	return series
}

func sortedKeys[V any](values map[string]V) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
