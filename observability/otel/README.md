# OpenTelemetry

Go и Python используют OpenTelemetry SDK и отправляют OTLP/gRPC spans напрямую в локальный Jaeger. HTTP transport распространяет W3C `traceparent`; event envelope сохраняет originating `trace_id`/`span_id`, поэтому Kafka consumers продолжают исходный checkout trace после outbox-delivery.

Локально используется 100% sampling для наглядной демонстрации. В production-like окружении `FRESHFLOW_OTEL_SAMPLE_RATIO` должен быть уменьшен, а exporter обычно направляется в OpenTelemetry Collector.
