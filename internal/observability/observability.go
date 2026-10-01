package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
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
		"result", "operation", "groupId":
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
	fields["errorClass"] = classifyError(err)
	l.Log(ctx, "error", message, fields)
}

func classifyError(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%T", err)
}

type Metrics struct {
	mu       sync.RWMutex
	counters map[string]uint64
	hist     map[string]histogram
}

type histogram struct {
	count uint64
	sum   float64
}

func NewMetrics() *Metrics {
	return &Metrics{counters: make(map[string]uint64), hist: make(map[string]histogram)}
}

func (m *Metrics) Inc(name string) {
	m.mu.Lock()
	m.counters[name]++
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
		for _, name := range sortedKeys(m.counters) {
			fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", name, name, m.counters[name])
		}
		for _, name := range sortedHistogramKeys(m.hist) {
			value := m.hist[name]
			fmt.Fprintf(w, "# TYPE %s_seconds summary\n%s_seconds_count %d\n%s_seconds_sum %f\n", name, name, value.count, name, value.sum)
		}
	})
}

func (m *Metrics) Snapshot(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.counters[name]
}

func sortedKeys(values map[string]uint64) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func sortedHistogramKeys(values map[string]histogram) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
