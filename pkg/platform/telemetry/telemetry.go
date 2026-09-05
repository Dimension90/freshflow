package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var (
	httpDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "http_server_request_duration_seconds", Help: "HTTP server request duration.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"service", "route", "method", "status"})
	httpErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "http_server_errors_total", Help: "HTTP responses with status 4xx or 5xx.",
	}, []string{"service", "route", "method", "status"})
	ordersCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "freshflow_orders_created_total", Help: "Orders successfully committed by order-service.",
	})
	kafkaLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "freshflow_kafka_consumer_lag", Help: "Approximate Kafka consumer lag at the last processed record.",
	}, []string{"service", "topic", "partition"})
	etaDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "freshflow_eta_prediction_duration_seconds", Help: "ETA prediction request duration.",
		Buckets: prometheus.DefBuckets,
	}, []string{"caller", "status"})

	initOnce sync.Once
	initErr  error
	provider *sdktrace.TracerProvider
)

func init() {
	prometheus.MustRegister(httpDuration, httpErrors, ordersCreated, kafkaLag, etaDuration)
}

func Init(ctx context.Context, service, environment string, logger *slog.Logger) error {
	initOnce.Do(func() {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		endpoint := os.Getenv("FRESHFLOW_OTEL_EXPORTER_OTLP_ENDPOINT")
		if endpoint == "" {
			return
		}
		exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			initErr = fmt.Errorf("create OTLP trace exporter: %w", err)
			return
		}
		sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRatio()))
		provider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter), sdktrace.WithSampler(sampler),
			sdktrace.WithResource(resource.NewSchemaless(
				attribute.String("service.name", service),
				attribute.String("deployment.environment", environment),
			)),
		)
		otel.SetTracerProvider(provider)
		logger.Info("OpenTelemetry tracing enabled", "otlp_endpoint", endpoint)
	})
	return initErr
}

func Shutdown(ctx context.Context) error {
	if provider == nil {
		return nil
	}
	return provider.Shutdown(ctx)
}

func MetricsHandler() http.Handler { return promhttp.Handler() }

func ObserveHTTP(service, route, method string, status int, duration time.Duration) {
	if route == "" {
		route = "unmatched"
	}
	statusText := strconv.Itoa(status)
	httpDuration.WithLabelValues(service, route, method, statusText).Observe(duration.Seconds())
	if status >= 400 {
		httpErrors.WithLabelValues(service, route, method, statusText).Inc()
	}
}

func IncOrdersCreated() { ordersCreated.Inc() }

func ObserveKafkaRecord(service, topic string, partition int32, offset, highWatermark int64) {
	lag := highWatermark - offset - 1
	if lag < 0 {
		lag = 0
	}
	kafkaLag.WithLabelValues(service, topic, strconv.FormatInt(int64(partition), 10)).Set(float64(lag))
}

func ObserveKafkaFetches(service string, fetches kgo.Fetches) {
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if len(partition.Records) == 0 {
			return
		}
		last := partition.Records[len(partition.Records)-1]
		ObserveKafkaRecord(service, partition.Topic, partition.Partition, last.Offset, partition.HighWatermark)
	})
}

func ObserveETAPrediction(caller, status string, duration time.Duration) {
	etaDuration.WithLabelValues(caller, status).Observe(duration.Seconds())
}

func StartConsumerSpan(ctx context.Context, service, eventType, traceID, spanID string) (context.Context, trace.Span) {
	parsedTraceID, traceErr := trace.TraceIDFromHex(traceID)
	parsedSpanID, spanErr := trace.SpanIDFromHex(spanID)
	if traceErr == nil && spanErr == nil {
		parent := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: parsedTraceID, SpanID: parsedSpanID, TraceFlags: trace.FlagsSampled, Remote: true,
		})
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	return otel.Tracer(service).Start(ctx, "consume "+eventType, trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attribute.String("messaging.system", "kafka"), attribute.String("messaging.operation", "process"), attribute.String("messaging.message.type", eventType)))
}

func TraceFields(ctx context.Context) []any {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return nil
	}
	return []any{"trace_id", spanContext.TraceID().String(), "span_id", spanContext.SpanID().String()}
}

func sampleRatio() float64 {
	raw := os.Getenv("FRESHFLOW_OTEL_SAMPLE_RATIO")
	if raw == "" {
		return 1
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value < 0 || value > 1 {
		return 1
	}
	return value
}
