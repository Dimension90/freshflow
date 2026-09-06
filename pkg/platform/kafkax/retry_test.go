package kafkax

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProcessDeadLettersPoisonRecordAfterBoundedRetries(t *testing.T) {
	record := &kgo.Record{Topic: "freshflow.order.events.v1", Partition: 2, Offset: 11, Value: []byte("not-json")}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handleCalls := 0
	var published DeadLetter

	outcome, err := ProcessWithPublisher(context.Background(), "test-consumer", nil, record, logger,
		func(context.Context) error {
			handleCalls++
			return errors.New("invalid envelope")
		},
		func(_ context.Context, _ *kgo.Client, key []byte, deadLetter DeadLetter) error {
			published = deadLetter
			if string(key) != "" {
				t.Fatalf("unexpected key %q", key)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("ProcessWithPublisher() error = %v", err)
	}
	if outcome != DeadLettered {
		t.Fatalf("outcome = %q, want %q", outcome, DeadLettered)
	}
	if handleCalls != maxAttempts {
		t.Fatalf("handler calls = %d, want %d", handleCalls, maxAttempts)
	}
	if published.SourceTopic != record.Topic || published.SourcePartition != record.Partition || published.Attempts != maxAttempts {
		t.Fatalf("unexpected dead letter: %#v", published)
	}
	if got := published.OriginalValueBase64; got != base64.StdEncoding.EncodeToString(record.Value) {
		t.Fatalf("original value = %q", got)
	}
}

func TestDLQTopic(t *testing.T) {
	if got := DLQTopic("freshflow.delivery.events.v1"); got != "freshflow.delivery.events.v1.dlq" {
		t.Fatalf("DLQTopic() = %q", got)
	}
}

func TestDecodeDeadLetter(t *testing.T) {
	value := []byte(`{"source_topic":"freshflow.order.events.v1","original_value_base64":"aGVsbG8="}`)
	deadLetter, original, err := DecodeDeadLetter(value)
	if err != nil {
		t.Fatalf("DecodeDeadLetter() error = %v", err)
	}
	if deadLetter.SourceTopic != "freshflow.order.events.v1" || string(original) != "hello" {
		t.Fatalf("DecodeDeadLetter() = %#v, %q", deadLetter, original)
	}
}

func TestDecodeDeadLetterRejectsInvalidEnvelope(t *testing.T) {
	for _, value := range [][]byte{
		[]byte(`{"source_topic":"","original_value_base64":"aGVsbG8="}`),
		[]byte(`{"source_topic":"freshflow.order.events.v1.dlq","original_value_base64":"aGVsbG8="}`),
		[]byte(`{"source_topic":"freshflow.order.events.v1","original_value_base64":"not-base64"}`),
	} {
		if _, _, err := DecodeDeadLetter(value); err == nil {
			t.Fatalf("DecodeDeadLetter(%s) error = nil", value)
		}
	}
}
