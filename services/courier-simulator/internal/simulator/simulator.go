package simulator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/events"
	"github.com/freshflow/freshflow/pkg/platform/httpx"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

const courierGeoKey = "local:couriers:geo"

type ReadinessError struct{ Status int }

func (e *ReadinessError) Error() string { return fmt.Sprintf("readiness status %d", e.Status) }

type courier struct {
	ID        string
	Latitude  float64
	Longitude float64
}

var couriers = []courier{
	{ID: "40000000-0000-4000-8000-000000000001", Latitude: 55.0411, Longitude: 82.9207},
	{ID: "40000000-0000-4000-8000-000000000002", Latitude: 55.0299, Longitude: 82.9234},
	{ID: "40000000-0000-4000-8000-000000000003", Latitude: 55.0378, Longitude: 82.9581},
}

type assignment struct {
	ID                     string    `json:"id"`
	CourierID              string    `json:"courier_id"`
	PickupLatitude         float64   `json:"pickup_latitude"`
	PickupLongitude        float64   `json:"pickup_longitude"`
	DestinationLatitude    float64   `json:"destination_latitude"`
	DestinationLongitude   float64   `json:"destination_longitude"`
	AssignedAt             time.Time `json:"assigned_at"`
	SimulationDelaySeconds int       `json:"simulation_delay_seconds"`
	CorrelationID          string    `json:"correlation_id"`
}

type Runner struct {
	redis       *redis.Client
	kafka       *kgo.Client
	http        *http.Client
	deliveryURL string
	logger      *slog.Logger
	sequence    int64
}

func New(redisClient *redis.Client, kafka *kgo.Client, httpClient *http.Client, deliveryURL string, logger *slog.Logger) *Runner {
	return &Runner{redis: redisClient, kafka: kafka, http: httpClient, deliveryURL: deliveryURL, logger: logger, sequence: time.Now().UnixMilli()}
}

func (r *Runner) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		r.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) tick(ctx context.Context) {
	assignments, err := r.fetchAssignments(ctx)
	if err != nil {
		r.logger.Warn("fetch active deliveries", "error", err)
		assignments = nil
	}
	byCourier := make(map[string]assignment, len(assignments))
	for _, item := range assignments {
		byCourier[item.CourierID] = item
	}
	now := time.Now().UTC()
	for index, item := range couriers {
		location := simulatedLocation(item, byCourier[item.ID], now, index)
		r.sequence++
		location.Sequence = r.sequence
		if err := r.publish(ctx, location); err != nil {
			r.logger.Warn("publish courier location", "courier_id", item.ID, "error", err)
			continue
		}
		_ = r.redis.GeoAdd(ctx, courierGeoKey, &redis.GeoLocation{Name: item.ID, Longitude: location.Longitude, Latitude: location.Latitude}).Err()
		if encoded, err := json.Marshal(location); err == nil {
			_ = r.redis.Set(ctx, "local:courier:state:"+item.ID, encoded, 10*time.Second).Err()
		}
	}
}

type location struct {
	CourierID      string    `json:"courier_id"`
	DeliveryID     string    `json:"delivery_id,omitempty"`
	Latitude       float64   `json:"latitude"`
	Longitude      float64   `json:"longitude"`
	HeadingDegrees float64   `json:"heading_degrees"`
	SpeedMPS       float64   `json:"speed_mps"`
	RecordedAt     time.Time `json:"recorded_at"`
	Sequence       int64     `json:"sequence"`
	Phase          string    `json:"phase"`
	CorrelationID  string    `json:"-"`
}

func simulatedLocation(base courier, active assignment, now time.Time, index int) location {
	result := location{CourierID: base.ID, Latitude: base.Latitude, Longitude: base.Longitude, RecordedAt: now, Phase: "available"}
	if active.ID == "" {
		angle := float64(now.Unix()%60)/60*2*math.Pi + float64(index)
		result.Latitude += math.Sin(angle) * 0.0003
		result.Longitude += math.Cos(angle) * 0.0003
		result.HeadingDegrees = math.Mod(angle*180/math.Pi+360, 360)
		result.SpeedMPS = 1.2
		return result
	}
	result.DeliveryID = active.ID
	result.CorrelationID = active.CorrelationID
	elapsed := now.Sub(active.AssignedAt) - time.Duration(active.SimulationDelaySeconds)*time.Second
	if elapsed < 3*time.Second {
		result.Phase = "assembling"
		result.Latitude, result.Longitude = active.PickupLatitude, active.PickupLongitude
		return result
	}
	if elapsed < 15*time.Second {
		result.Phase = "delivering"
		progress := (elapsed - 3*time.Second).Seconds() / (12 * time.Second).Seconds()
		result.Latitude = active.PickupLatitude + (active.DestinationLatitude-active.PickupLatitude)*progress
		result.Longitude = active.PickupLongitude + (active.DestinationLongitude-active.PickupLongitude)*progress
		result.HeadingDegrees = 45
		result.SpeedMPS = 8
		return result
	}
	result.Phase = "completed"
	result.Latitude, result.Longitude = active.DestinationLatitude, active.DestinationLongitude
	return result
}

func (r *Runner) fetchAssignments(ctx context.Context) ([]assignment, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, r.deliveryURL+"/internal/v1/simulator/assignments", nil)
	if err != nil {
		return nil, err
	}
	response, err := r.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("assignments status %d", response.StatusCode)
	}
	var payload struct {
		Assignments []assignment `json:"assignments"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return nil, err
	}
	return payload.Assignments, nil
}

func (r *Runner) publish(ctx context.Context, value location) error {
	eventCtx := httpx.WithCorrelationID(ctx, value.CorrelationID)
	envelope, err := events.New(eventCtx, "courier.location_updated", "courier-simulator", value.CourierID, value)
	if err != nil {
		return err
	}
	encoded, err := envelope.Marshal()
	if err != nil {
		return err
	}
	headers := []kgo.RecordHeader{
		{Key: "event_id", Value: []byte(envelope.EventID)},
		{Key: "event_type", Value: []byte(envelope.EventType)},
		{Key: "correlation_id", Value: []byte(envelope.CorrelationID)},
	}
	return r.kafka.ProduceSync(ctx, &kgo.Record{Topic: "freshflow.courier.location.v1", Key: []byte(value.CourierID), Value: encoded, Headers: headers}).FirstErr()
}
