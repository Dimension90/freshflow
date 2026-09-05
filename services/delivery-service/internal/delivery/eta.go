package delivery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/freshflow/freshflow/pkg/platform/id"
	"github.com/freshflow/freshflow/pkg/platform/telemetry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type ETAFeatures struct {
	DistanceKM        float64 `json:"distance_km"`
	ItemCount         int     `json:"item_count"`
	Stage             string  `json:"stage"`
	DistrictLoad      float64 `json:"district_load"`
	AvailableCouriers int     `json:"available_couriers"`
}

type ETAPrediction struct {
	PredictedETASeconds int       `json:"predicted_eta_seconds"`
	ModelVersion        string    `json:"model_version"`
	ComputedAt          time.Time `json:"computed_at"`
}

type ETAClient interface {
	Predict(context.Context, ETAFeatures) (ETAPrediction, error)
	Ready(context.Context) error
}

type HTTPETAClient struct {
	baseURL string
	http    *http.Client
}

func NewHTTPETAClient(baseURL string, timeout time.Duration) *HTTPETAClient {
	return &HTTPETAClient{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: timeout, Transport: otelhttp.NewTransport(http.DefaultTransport)}}
}

func (c *HTTPETAClient) Predict(ctx context.Context, features ETAFeatures) (ETAPrediction, error) {
	started := time.Now()
	status := "error"
	defer func() { telemetry.ObserveETAPrediction("delivery-service", status, time.Since(started)) }()
	encoded, err := json.Marshal(features)
	if err != nil {
		return ETAPrediction{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/predict-eta", bytes.NewReader(encoded))
	if err != nil {
		return ETAPrediction{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(httpx.CorrelationHeader, httpx.CorrelationID(ctx))
	response, err := c.http.Do(request)
	if err != nil {
		return ETAPrediction{}, fmt.Errorf("call ETA service: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return ETAPrediction{}, fmt.Errorf("ETA service status %d", response.StatusCode)
	}
	var prediction ETAPrediction
	if err := json.NewDecoder(response.Body).Decode(&prediction); err != nil {
		return ETAPrediction{}, fmt.Errorf("decode ETA response: %w", err)
	}
	if prediction.PredictedETASeconds <= 0 || prediction.ModelVersion == "" {
		return ETAPrediction{}, fmt.Errorf("ETA service returned an invalid prediction")
	}
	status = "success"
	return prediction, nil
}

func (c *HTTPETAClient) Ready(ctx context.Context) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/readyz", nil)
	if err != nil {
		return err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("ETA readiness status %d", response.StatusCode)
	}
	return nil
}

type predictionJob struct {
	DeliveryID, OrderID, Stage string
	TraceID, SpanID            string
	ItemCount                  int
	PickupLatitude             float64
	PickupLongitude            float64
	DestinationLatitude        float64
	DestinationLongitude       float64
	CourierLatitude            float64
	CourierLongitude           float64
}

type PredictionWorker struct {
	postgres *pgxpool.Pool
	eta      ETAClient
	logger   *slog.Logger
	interval time.Duration
}

func NewPredictionWorker(postgres *pgxpool.Pool, eta ETAClient, logger *slog.Logger, interval time.Duration) *PredictionWorker {
	return &PredictionWorker{postgres: postgres, eta: eta, logger: logger, interval: interval}
}

func (w *PredictionWorker) Run(ctx context.Context) {
	if w.interval <= 0 {
		w.interval = 3 * time.Second
	}
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		if err := w.predictMissing(ctx); err != nil && ctx.Err() == nil {
			w.logger.Warn("predict missing ETA", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *PredictionWorker) predictMissing(ctx context.Context) error {
	rows, err := w.postgres.Query(ctx, `
		SELECT d.id::text, d.order_id::text,
		       CASE d.status WHEN 'assigned' THEN 'confirmed' ELSE d.status END AS stage,
		       d.item_count, d.trace_id, d.origin_span_id, d.pickup_latitude, d.pickup_longitude,
		       d.destination_latitude, d.destination_longitude, c.latitude, c.longitude
		FROM delivery.deliveries d
		JOIN delivery.couriers c ON c.id = d.courier_id
		WHERE d.status NOT IN ('completed', 'cancelled')
		  AND NOT EXISTS (
		      SELECT 1 FROM delivery.eta_predictions p
		      WHERE p.delivery_id = d.id
		        AND p.stage = CASE d.status WHEN 'assigned' THEN 'confirmed' ELSE d.status END
		  )
		ORDER BY d.assigned_at LIMIT 50`)
	if err != nil {
		return err
	}
	jobs := make([]predictionJob, 0)
	for rows.Next() {
		var job predictionJob
		if err := rows.Scan(&job.DeliveryID, &job.OrderID, &job.Stage, &job.ItemCount, &job.TraceID, &job.SpanID,
			&job.PickupLatitude, &job.PickupLongitude, &job.DestinationLatitude, &job.DestinationLongitude,
			&job.CourierLatitude, &job.CourierLongitude); err != nil {
			rows.Close()
			return err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()
	if len(jobs) == 0 {
		return nil
	}
	var activeDeliveries, availableCouriers int
	if err := w.postgres.QueryRow(ctx, `SELECT count(*) FROM delivery.deliveries WHERE status NOT IN ('completed', 'cancelled')`).Scan(&activeDeliveries); err != nil {
		return err
	}
	if err := w.postgres.QueryRow(ctx, `SELECT count(*) FROM delivery.couriers WHERE status = 'available'`).Scan(&availableCouriers); err != nil {
		return err
	}
	districtLoad := math.Min(3, 1+float64(activeDeliveries)*0.1)
	for _, job := range jobs {
		distance := haversineKM(job.CourierLatitude, job.CourierLongitude, job.DestinationLatitude, job.DestinationLongitude)
		if job.Stage != "delivering" {
			distance = haversineKM(job.CourierLatitude, job.CourierLongitude, job.PickupLatitude, job.PickupLongitude) +
				haversineKM(job.PickupLatitude, job.PickupLongitude, job.DestinationLatitude, job.DestinationLongitude)
		}
		features := ETAFeatures{DistanceKM: distance, ItemCount: job.ItemCount, Stage: job.Stage,
			DistrictLoad: districtLoad, AvailableCouriers: availableCouriers}
		jobCtx := httpx.WithCorrelationID(ctx, "eta-"+job.OrderID)
		jobCtx, span := telemetry.StartConsumerSpan(jobCtx, "delivery-service", "eta.predict."+job.Stage, job.TraceID, job.SpanID)
		prediction, err := w.eta.Predict(jobCtx, features)
		if err != nil {
			span.RecordError(err)
			span.End()
			w.logger.Warn("ETA prediction failed", "order_id", job.OrderID, "stage", job.Stage, "error", err)
			continue
		}
		if err := w.save(jobCtx, job, features, prediction); err != nil {
			span.RecordError(err)
			span.End()
			return err
		}
		span.End()
	}
	return nil
}

func (w *PredictionWorker) save(ctx context.Context, job predictionJob, features ETAFeatures, prediction ETAPrediction) error {
	predictionID, err := id.NewUUID()
	if err != nil {
		return err
	}
	if prediction.ComputedAt.IsZero() {
		prediction.ComputedAt = time.Now().UTC()
	}
	tx, err := w.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	result, err := tx.Exec(ctx, `
		INSERT INTO delivery.eta_predictions
		(id, delivery_id, order_id, stage, distance_km, item_count, district_load, available_couriers,
		 predicted_eta_seconds, actual_eta_seconds, model_version, predicted_at, completed_at)
		SELECT $1, d.id, $3, $4, $5, $6, $7, $8, $9,
		       CASE WHEN d.completed_at IS NULL THEN NULL
		            ELSE GREATEST(0, EXTRACT(EPOCH FROM (d.completed_at - $11::timestamptz))::integer) END,
		       $10, $11, d.completed_at
		FROM delivery.deliveries d WHERE d.id = $2
		ON CONFLICT (delivery_id, stage) DO NOTHING`, predictionID, job.DeliveryID, job.OrderID, job.Stage,
		features.DistanceKM, features.ItemCount, features.DistrictLoad, features.AvailableCouriers,
		prediction.PredictedETASeconds, prediction.ModelVersion, prediction.ComputedAt)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 1 {
		_, err = tx.Exec(ctx, `UPDATE delivery.deliveries
			SET predicted_eta_seconds=$2, eta_model_version=$3, eta_updated_at=$4, updated_at=now()
			WHERE id=$1`, job.DeliveryID, prediction.PredictedETASeconds, prediction.ModelVersion, prediction.ComputedAt)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	w.logger.Info("ETA prediction stored", "order_id", job.OrderID, "delivery_id", job.DeliveryID,
		"stage", job.Stage, "predicted_eta_seconds", prediction.PredictedETASeconds, "model_version", prediction.ModelVersion)
	return nil
}

func haversineKM(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKM = 6371.0
	toRadians := math.Pi / 180
	dLat := (lat2 - lat1) * toRadians
	dLon := (lon2 - lon1) * toRadians
	a := math.Sin(dLat/2)*math.Sin(dLat/2) + math.Cos(lat1*toRadians)*math.Cos(lat2*toRadians)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return earthRadiusKM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}
