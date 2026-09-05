package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	demoUserID    = "00000000-0000-4000-8000-000000000001"
	demoProductID = "10000000-0000-4000-8000-000000000001"
)

func TestCatalogCartCheckoutAndIdempotency(t *testing.T) {
	if os.Getenv("FRESHFLOW_INTEGRATION") != "1" {
		t.Skip("set FRESHFLOW_INTEGRATION=1 with the Compose stack running")
	}
	baseURL := os.Getenv("FRESHFLOW_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	client := &http.Client{Timeout: 5 * time.Second}

	var catalog struct {
		Products []struct {
			ID string `json:"id"`
		} `json:"products"`
	}
	requestJSON(t, client, http.MethodGet, baseURL+"/api/v1/catalog/products", nil, nil, http.StatusOK, &catalog)
	if len(catalog.Products) < 1 {
		t.Fatal("catalog is empty")
	}

	var cart struct {
		Version int64 `json:"version"`
	}
	requestJSON(t, client, http.MethodPut, baseURL+"/api/v1/carts/"+demoUserID+"/items/"+demoProductID,
		map[string]any{"quantity": 1}, nil, http.StatusOK, &cart)
	if cart.Version < 1 {
		t.Fatalf("cart version = %d", cart.Version)
	}

	idempotencyKey := fmt.Sprintf("integration-%d", time.Now().UnixNano())
	headers := map[string]string{"Idempotency-Key": idempotencyKey}
	payload := map[string]any{"user_id": demoUserID, "cart_version": cart.Version}
	var created struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	createdResponse := requestJSON(t, client, http.MethodPost, baseURL+"/api/v1/orders", payload, headers, http.StatusCreated, &created)
	if created.ID == "" || created.Status != "created" {
		t.Fatalf("unexpected created order: %#v", created)
	}
	orderEventID := assertDomainEventsAndNotificationDeduplication(t, createdResponse.Header.Get("X-Correlation-ID"), created.ID)

	var replayed struct {
		ID string `json:"id"`
	}
	response := requestJSON(t, client, http.MethodPost, baseURL+"/api/v1/orders", payload, headers, http.StatusCreated, &replayed)
	if replayed.ID != created.ID || response.Header.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("idempotency replay mismatch: id=%q header=%q", replayed.ID, response.Header.Get("Idempotency-Replayed"))
	}

	conflicting := map[string]any{"user_id": demoUserID, "cart_version": cart.Version + 1}
	requestJSON(t, client, http.MethodPost, baseURL+"/api/v1/orders", conflicting, headers, http.StatusConflict, nil)
	requestJSON(t, client, http.MethodGet, baseURL+"/api/v1/orders/"+created.ID, nil, nil, http.StatusOK, nil)
	waitForDelivered(t, client, baseURL, created.ID)
	assertETAPredictions(t, created.ID)
	assertAnalyticsProjection(t, created.ID, orderEventID)
}

func assertETAPredictions(t *testing.T, orderID string) {
	t.Helper()
	dsn := os.Getenv("FRESHFLOW_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	postgres, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer postgres.Close()
	var predictions, evaluated int
	if err := postgres.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE actual_eta_seconds IS NOT NULL)
		FROM delivery.eta_predictions WHERE order_id = $1`, orderID).Scan(&predictions, &evaluated); err != nil {
		t.Fatal(err)
	}
	if predictions < 1 || evaluated != predictions {
		t.Fatalf("ETA predictions are not evaluation-ready: predictions=%d evaluated=%d", predictions, evaluated)
	}
}

func waitForDelivered(t *testing.T, client *http.Client, baseURL, orderID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var order struct {
			Status string `json:"status"`
		}
		requestJSON(t, client, http.MethodGet, baseURL+"/api/v1/orders/"+orderID, nil, nil, http.StatusOK, &order)
		if order.Status == "delivered" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("order did not reach delivered; last status=%s", order.Status)
		}
		time.Sleep(500 * time.Millisecond)
	}
	var delivery struct {
		Status string `json:"status"`
	}
	requestJSON(t, client, http.MethodGet, baseURL+"/api/v1/deliveries/order/"+orderID, nil, nil, http.StatusOK, &delivery)
	if delivery.Status != "completed" {
		t.Fatalf("delivery status = %s, want completed", delivery.Status)
	}
}

func assertDomainEventsAndNotificationDeduplication(t *testing.T, correlationID, orderID string) string {
	t.Helper()
	broker := os.Getenv("FRESHFLOW_KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(fmt.Sprintf("freshflow-integration-%d", time.Now().UnixNano())),
		kgo.ConsumeTopics("freshflow.order.events.v1", "freshflow.inventory.events.v1"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	foundOrder, foundInventory := false, false
	var orderEvent events.Envelope
	var orderEventValue []byte
	for !foundOrder || !foundInventory {
		fetches := client.PollRecords(ctx, 100)
		if err := fetches.Err(); err != nil {
			t.Fatalf("poll domain events: %v", err)
		}
		for _, record := range fetches.Records() {
			var envelope events.Envelope
			if json.Unmarshal(record.Value, &envelope) != nil || envelope.CorrelationID != correlationID {
				continue
			}
			switch envelope.EventType {
			case "order.created":
				if envelope.AggregateID == orderID {
					foundOrder = true
					orderEvent = envelope
					orderEventValue = append([]byte(nil), record.Value...)
				}
			case "inventory.reserved":
				foundInventory = true
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("expected domain events were not observed: order=%v inventory=%v", foundOrder, foundInventory)
		}
	}
	if len(orderEvent.TraceID) != 32 || len(orderEvent.SpanID) != 16 {
		t.Fatalf("order.created has no valid trace context: trace_id=%q span_id=%q", orderEvent.TraceID, orderEvent.SpanID)
	}

	if err := client.ProduceSync(ctx, &kgo.Record{
		Topic: "freshflow.order.events.v1", Key: []byte(orderID), Value: orderEventValue,
	}).FirstErr(); err != nil {
		t.Fatalf("publish duplicate event: %v", err)
	}

	dsn := os.Getenv("FRESHFLOW_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://freshflow:freshflow@localhost:5432/freshflow?sslmode=disable"
	}
	dbCtx, cancelDB := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelDB()
	postgres, err := pgxpool.New(dbCtx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer postgres.Close()
	deadline := time.Now().Add(9 * time.Second)
	for {
		var count int
		if err := postgres.QueryRow(dbCtx, `SELECT count(*) FROM notifications.notification_log WHERE event_id = $1`, orderEvent.EventID).Scan(&count); err != nil {
			t.Fatalf("query notification log: %v", err)
		}
		if count == 1 {
			time.Sleep(500 * time.Millisecond)
			if err := postgres.QueryRow(dbCtx, `SELECT count(*) FROM notifications.notification_log WHERE event_id = $1`, orderEvent.EventID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("duplicate notification detected: count=%d error=%v", count, err)
			}
			return orderEvent.EventID
		}
		if time.Now().After(deadline) {
			t.Fatal("notification worker did not record order.created")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func assertAnalyticsProjection(t *testing.T, orderID, orderEventID string) {
	t.Helper()
	address := os.Getenv("FRESHFLOW_CLICKHOUSE_ADDR")
	if address == "" {
		address = "localhost:9000"
	}
	connection, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{address},
		Auth:        clickhouse.Auth{Database: "freshflow", Username: "freshflow", Password: "freshflow"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	deadline := time.Now().Add(14 * time.Second)
	for {
		var createdRows, completedRows uint64
		err = connection.QueryRow(ctx, `SELECT count() FROM order_analytics FINAL WHERE event_id = ?`, uuid.MustParse(orderEventID)).Scan(&createdRows)
		if err == nil {
			err = connection.QueryRow(ctx, `SELECT count() FROM order_analytics FINAL WHERE order_id = ? AND event_type = 'delivery.completed'`, uuid.MustParse(orderID)).Scan(&completedRows)
		}
		if err == nil && createdRows == 1 && completedRows == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("analytics projection incomplete: order.created rows=%d delivery.completed rows=%d error=%v", createdRows, completedRows, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, url string, payload any, headers map[string]string, expectedStatus int, destination any) *http.Response {
	t.Helper()
	var body bytes.Buffer
	if payload != nil {
		if err := json.NewEncoder(&body).Encode(payload); err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, url, &body)
	if err != nil {
		t.Fatal(err)
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		var errorBody any
		_ = json.NewDecoder(response.Body).Decode(&errorBody)
		t.Fatalf("%s %s status = %d, want %d, body=%#v", method, url, response.StatusCode, expectedStatus, errorBody)
	}
	if destination != nil {
		if err := json.NewDecoder(response.Body).Decode(destination); err != nil {
			t.Fatalf("decode %s %s: %v", method, url, err)
		}
	}
	return response
}
