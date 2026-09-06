package kafkax

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	maxAttempts = 3
	maxErrorLen = 512
)

type Outcome string

const (
	Processed    Outcome = "processed"
	DeadLettered Outcome = "dead_lettered"
)

// DeadLetter preserves the original record even when it is invalid JSON.
type DeadLetter struct {
	SourceTopic         string    `json:"source_topic"`
	SourcePartition     int32     `json:"source_partition"`
	SourceOffset        int64     `json:"source_offset"`
	Consumer            string    `json:"consumer"`
	Attempts            int       `json:"attempts"`
	FailureReason       string    `json:"failure_reason"`
	FailedAt            time.Time `json:"failed_at"`
	OriginalValueBase64 string    `json:"original_value_base64"`
}

type Handler func(context.Context) error
type Publisher func(context.Context, *kgo.Client, []byte, DeadLetter) error

// Process retries a handler with bounded exponential backoff. Once its retry
// budget is exhausted, it blocks the source partition only until the original
// record is durably written to the corresponding DLQ.
func Process(ctx context.Context, service string, client *kgo.Client, record *kgo.Record, logger *slog.Logger, handle Handler) (Outcome, error) {
	return ProcessWithPublisher(ctx, service, client, record, logger, handle, Publish)
}

// ProcessWithPublisher is Process with an injectable publisher for focused
// tests; production consumers use Process.
func ProcessWithPublisher(ctx context.Context, service string, client *kgo.Client, record *kgo.Record, logger *slog.Logger, handle Handler, publish Publisher) (Outcome, error) {
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := handle(ctx); err == nil {
			return Processed, nil
		} else {
			lastErr = err
		}
		if attempt == maxAttempts {
			break
		}
		telemetry.IncKafkaRetry(service, record.Topic)
		logger.Warn("Kafka handler failed; retrying", "topic", record.Topic, "partition", record.Partition,
			"offset", record.Offset, "attempt", attempt, "error", lastErr)
		if err := sleep(ctx, retryDelay(attempt, record.Offset)); err != nil {
			return "", err
		}
	}

	dlq := DeadLetter{
		SourceTopic: record.Topic, SourcePartition: record.Partition, SourceOffset: record.Offset,
		Consumer: service, Attempts: maxAttempts, FailureReason: sanitizeError(lastErr),
		FailedAt: time.Now().UTC(), OriginalValueBase64: base64.StdEncoding.EncodeToString(record.Value),
	}
	for publishAttempt := 1; ; publishAttempt++ {
		if err := publish(ctx, client, record.Key, dlq); err == nil {
			telemetry.IncKafkaDeadLetter(service, record.Topic)
			logger.Error("Kafka record sent to DLQ", "topic", record.Topic, "dlq_topic", DLQTopic(record.Topic),
				"partition", record.Partition, "offset", record.Offset, "attempts", maxAttempts, "error", lastErr)
			return DeadLettered, nil
		} else {
			telemetry.IncKafkaDLQPublishFailure(service, record.Topic)
			logger.Error("publish Kafka dead letter failed; retrying", "topic", record.Topic, "partition", record.Partition,
				"offset", record.Offset, "attempt", publishAttempt, "error", err)
		}
		if err := sleep(ctx, retryDelay(publishAttempt, record.Offset)); err != nil {
			return "", err
		}
	}
}

func DLQTopic(sourceTopic string) string { return sourceTopic + ".dlq" }

// DecodeDeadLetter validates a DLQ envelope and restores its original value.
// The value is base64 encoded because consumers must be able to isolate even
// malformed JSON records without losing their bytes.
func DecodeDeadLetter(value []byte) (DeadLetter, []byte, error) {
	var deadLetter DeadLetter
	if err := json.Unmarshal(value, &deadLetter); err != nil {
		return DeadLetter{}, nil, fmt.Errorf("decode dead letter: %w", err)
	}
	if strings.TrimSpace(deadLetter.SourceTopic) == "" {
		return DeadLetter{}, nil, fmt.Errorf("decode dead letter: source_topic is required")
	}
	if strings.HasSuffix(deadLetter.SourceTopic, ".dlq") {
		return DeadLetter{}, nil, fmt.Errorf("decode dead letter: source_topic must not be a DLQ topic")
	}
	original, err := base64.StdEncoding.DecodeString(deadLetter.OriginalValueBase64)
	if err != nil {
		return DeadLetter{}, nil, fmt.Errorf("decode dead letter original value: %w", err)
	}
	return deadLetter, original, nil
}

func Publish(ctx context.Context, client *kgo.Client, key []byte, deadLetter DeadLetter) error {
	value, err := json.Marshal(deadLetter)
	if err != nil {
		return fmt.Errorf("encode dead letter: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return client.ProduceSync(publishCtx, &kgo.Record{Topic: DLQTopic(deadLetter.SourceTopic), Key: key, Value: value}).FirstErr()
}

func retryDelay(attempt int, offset int64) time.Duration {
	if attempt > 5 {
		attempt = 5
	}
	base := 200 * time.Millisecond * time.Duration(1<<(attempt-1))
	jitter := time.Duration((offset+int64(attempt)*37)%100) * time.Millisecond
	return base + jitter
}

func sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func sanitizeError(err error) string {
	if err == nil {
		return "unknown consumer failure"
	}
	message := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(message) > maxErrorLen {
		return message[:maxErrorLen]
	}
	return message
}
