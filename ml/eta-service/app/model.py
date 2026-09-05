from __future__ import annotations

import math
from dataclasses import dataclass


MODEL_VERSION = "baseline-v1"


@dataclass(frozen=True)
class Prediction:
    seconds: int
    travel_seconds: int
    handling_seconds: int
    load_penalty_seconds: int
    availability_penalty_seconds: int


def predict(
    *,
    distance_km: float,
    item_count: int,
    stage: str,
    district_load: float,
    available_couriers: int,
) -> Prediction:
    """Return a transparent baseline that can later be replaced by a trained model."""
    speed_kmh = 22.0 if stage == "delivering" else 18.0
    travel_seconds = distance_km / speed_kmh * 3600.0

    base_handling, per_item = {
        "created": (300.0, 45.0),
        "confirmed": (240.0, 40.0),
        "assembling": (90.0, 20.0),
        "delivering": (0.0, 0.0),
    }[stage]
    handling_seconds = base_handling + item_count * per_item
    load_multiplier = min(max(district_load, 0.5), 3.0)
    load_penalty = (travel_seconds + handling_seconds) * (load_multiplier - 1.0)
    availability_penalty = {0: 600.0, 1: 300.0, 2: 120.0}.get(available_couriers, 0.0)
    total = max(60.0, travel_seconds + handling_seconds + load_penalty + availability_penalty)

    return Prediction(
        seconds=math.ceil(total),
        travel_seconds=round(travel_seconds),
        handling_seconds=round(handling_seconds),
        load_penalty_seconds=round(load_penalty),
        availability_penalty_seconds=round(availability_penalty),
    )
