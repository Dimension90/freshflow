package delivery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/freshflow/freshflow/pkg/platform/httpx"
)

func TestHTTPETAClientPredict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/predict-eta" || r.Header.Get(httpx.CorrelationHeader) != "order-42" {
			t.Fatalf("unexpected request: path=%s correlation=%s", r.URL.Path, r.Header.Get(httpx.CorrelationHeader))
		}
		var features ETAFeatures
		if err := json.NewDecoder(r.Body).Decode(&features); err != nil {
			t.Fatal(err)
		}
		if features.Stage != "assembling" || features.ItemCount != 3 {
			t.Fatalf("unexpected features: %#v", features)
		}
		_ = json.NewEncoder(w).Encode(ETAPrediction{PredictedETASeconds: 420, ModelVersion: "baseline-v1", ComputedAt: time.Now()})
	}))
	defer server.Close()

	client := NewHTTPETAClient(server.URL, time.Second)
	ctx := httpx.WithCorrelationID(context.Background(), "order-42")
	prediction, err := client.Predict(ctx, ETAFeatures{DistanceKM: 2.5, ItemCount: 3, Stage: "assembling", DistrictLoad: 1.1, AvailableCouriers: 2})
	if err != nil {
		t.Fatalf("Predict() error = %v", err)
	}
	if prediction.PredictedETASeconds != 420 {
		t.Fatalf("prediction = %#v", prediction)
	}
}

func TestHaversineKM(t *testing.T) {
	if distance := haversineKM(55.0302, 82.9204, 55.0302, 82.9204); distance != 0 {
		t.Fatalf("same point distance = %f", distance)
	}
	distance := haversineKM(55.0302, 82.9204, 55.0415, 82.9346)
	if distance < 1 || distance > 2 {
		t.Fatalf("unexpected Novosibirsk distance = %f", distance)
	}
}
