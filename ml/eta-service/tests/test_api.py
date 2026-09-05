from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_predict_eta_contract():
    response = client.post("/predict-eta", json={
        "distance_km": 4.2,
        "item_count": 5,
        "stage": "assembling",
        "district_load": 1.25,
        "available_couriers": 2,
    }, headers={"X-Correlation-ID": "eta-test-1"})
    assert response.status_code == 200
    assert response.headers["X-Correlation-ID"] == "eta-test-1"
    body = response.json()
    assert body["predicted_eta_seconds"] > 0
    assert body["model_version"] == "baseline-v1"
    assert body["breakdown"]["travel_seconds"] > 0


def test_validation_uses_api_error_envelope():
    response = client.post("/predict-eta", json={"distance_km": -1})
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_failed"


def test_metrics_endpoint_exposes_eta_histogram():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "freshflow_eta_prediction_duration_seconds" in response.text
