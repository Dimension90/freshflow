// dlq-replay inspects or explicitly re-publishes records from a FreshFlow DLQ.
// It deliberately does not delete DLQ records: Kafka retention remains the
// audit trail, while normal consumer idempotency makes replay at-least-once.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/envconfig"
	"github.com/freshflow/freshflow/pkg/platform/kafkax"
	"github.com/freshflow/freshflow/pkg/platform/runhttp"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
)

const confirmationPhrase = "REPLAY"

type config struct {
	topic   string
	limit   int
	execute bool
	confirm string
	timeout time.Duration
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "dlq-replay:", err)
		os.Exit(1)
	}
}

func run(args []string, output *os.File) error {
	flags := flag.NewFlagSet("dlq-replay", flag.ContinueOnError)
	flags.SetOutput(output)
	config := config{}
	flags.StringVar(&config.topic, "topic", "", "DLQ topic to inspect or replay (required)")
	flags.IntVar(&config.limit, "limit", 10, "maximum number of DLQ records to process")
	flags.BoolVar(&config.execute, "execute", false, "publish records back to their original topic")
	flags.StringVar(&config.confirm, "confirm", "", "must equal REPLAY when --execute is used")
	flags.DurationVar(&config.timeout, "timeout", 30*time.Second, "maximum runtime")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := config.validate(); err != nil {
		return err
	}

	logger := runhttp.JSONLogger("dlq-replay", envconfig.String("APP_ENV", "local-compose"))
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := telemetry.Shutdown(ctx); err != nil {
			logger.Warn("flush telemetry", "error", err)
		}
	}()
	return consume(config, logger)
}

func (c config) validate() error {
	if !strings.HasSuffix(c.topic, ".dlq") || strings.TrimSuffix(c.topic, ".dlq") == "" {
		return errors.New("--topic must be a non-empty topic ending in .dlq")
	}
	if c.limit <= 0 {
		return errors.New("--limit must be positive")
	}
	if c.timeout <= 0 {
		return errors.New("--timeout must be positive")
	}
	if c.execute && c.confirm != confirmationPhrase {
		return fmt.Errorf("--execute requires --confirm %s", confirmationPhrase)
	}
	if !c.execute && c.confirm != "" {
		return errors.New("--confirm is only valid together with --execute")
	}
	return nil
}

func consume(config config, logger *slog.Logger) error {
	groupID := fmt.Sprintf("freshflow.dlq-replay.%d", time.Now().UTC().UnixNano())
	client, err := kgo.NewClient(
		kgo.SeedBrokers(envconfig.CSV("FRESHFLOW_KAFKA_BROKERS", "localhost:9092")...),
		kgo.ClientID("freshflow-dlq-replay"),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(config.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("create Kafka client: %w", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), config.timeout)
	defer cancel()
	processed := 0
	for processed < config.limit {
		fetches := client.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return fmt.Errorf("poll DLQ: %w", err)
		}
		var processErr error
		fetches.EachRecord(func(record *kgo.Record) {
			if processErr != nil || processed >= config.limit {
				return
			}
			deadLetter, original, err := kafkax.DecodeDeadLetter(record.Value)
			if err != nil {
				processErr = fmt.Errorf("invalid DLQ record at %s[%d:%d]: %w", record.Topic, record.Partition, record.Offset, err)
				return
			}
			if config.execute {
				if err := client.ProduceSync(ctx, &kgo.Record{Topic: deadLetter.SourceTopic, Key: record.Key, Value: original}).FirstErr(); err != nil {
					processErr = fmt.Errorf("replay %s[%d:%d] to %s: %w", record.Topic, record.Partition, record.Offset, deadLetter.SourceTopic, err)
					return
				}
				if err := client.CommitRecords(ctx, record); err != nil {
					processErr = fmt.Errorf("commit replayed DLQ record %s[%d:%d]: %w", record.Topic, record.Partition, record.Offset, err)
					return
				}
			}
			logger.Info("DLQ record inspected", "mode", mode(config.execute), "dlq_topic", record.Topic,
				"dlq_partition", record.Partition, "dlq_offset", record.Offset, "source_topic", deadLetter.SourceTopic,
				"source_partition", deadLetter.SourcePartition, "source_offset", deadLetter.SourceOffset,
				"consumer", deadLetter.Consumer, "attempts", deadLetter.Attempts, "failure_reason", deadLetter.FailureReason)
			processed++
		})
		if processErr != nil {
			return processErr
		}
	}
	logger.Info("DLQ replay finished", "mode", mode(config.execute), "dlq_topic", config.topic, "records", processed, "limit", config.limit)
	return nil
}

func mode(execute bool) string {
	if execute {
		return "replay"
	}
	return "dry-run"
}
