from app.model import predict


def test_prediction_increases_with_distance_and_load():
    base = predict(distance_km=2, item_count=3, stage="assembling", district_load=1, available_couriers=3)
    busy = predict(distance_km=5, item_count=3, stage="assembling", district_load=1.5, available_couriers=1)
    assert busy.seconds > base.seconds


def test_delivering_removes_handling_time():
    result = predict(distance_km=1, item_count=20, stage="delivering", district_load=1, available_couriers=5)
    assert result.handling_seconds == 0


def test_no_available_courier_adds_penalty():
    none = predict(distance_km=3, item_count=2, stage="confirmed", district_load=1, available_couriers=0)
    many = predict(distance_km=3, item_count=2, stage="confirmed", district_load=1, available_couriers=5)
    assert none.seconds - many.seconds == 600
