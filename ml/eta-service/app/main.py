from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field

from .model import MODEL_VERSION, predict


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("eta-service")
app = FastAPI(title="FreshFlow ETA Service", version="1.0.0")
HTTP_DURATION = Histogram(
    "http_server_request_duration_seconds",
    "HTTP server request duration.",
    ["service", "route", "method", "status"],
)
HTTP_ERRORS = Counter(
    "http_server_errors_total",
    "HTTP responses with status 4xx or 5xx.",
    ["service", "route", "method", "status"],
)
ETA_DURATION = Histogram(
    "freshflow_eta_prediction_duration_seconds",
    "ETA model calculation duration.",
    ["caller", "status"],
)


def configure_tracing() -> None:
    endpoint = os.getenv("FRESHFLOW_OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return
    if not endpoint.startswith(("http://", "https://")):
        endpoint = "http://" + endpoint
    provider = TracerProvider(resource=Resource.create({
        "service.name": "eta-service",
        "deployment.environment": os.getenv("FRESHFLOW_ENV", "local"),
    }))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app, tracer_provider=provider, excluded_urls="healthz,readyz,metrics")


configure_tracing()


class ETARequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    distance_km: float = Field(ge=0, le=500)
    item_count: int = Field(ge=1, le=500)
    stage: Literal["created", "confirmed", "assembling", "delivering"]
    district_load: float = Field(ge=0.5, le=3.0)
    available_couriers: int = Field(ge=0, le=10_000)


class Breakdown(BaseModel):
    travel_seconds: int
    handling_seconds: int
    load_penalty_seconds: int
    availability_penalty_seconds: int


class ETAResponse(BaseModel):
    predicted_eta_seconds: int
    model_version: str
    computed_at: datetime
    breakdown: Breakdown


@app.middleware("http")
async def request_context(request: Request, call_next):
    correlation_id = request.headers.get("X-Correlation-ID") or uuid.uuid4().hex
    request.state.correlation_id = correlation_id[:128]
    started = time.perf_counter()
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = request.state.correlation_id
    duration = time.perf_counter() - started
    route = getattr(request.scope.get("route"), "path", "unmatched")
    status = str(response.status_code)
    HTTP_DURATION.labels("eta-service", route, request.method, status).observe(duration)
    if response.status_code >= 400:
        HTTP_ERRORS.labels("eta-service", route, request.method, status).inc()
    span_context = trace.get_current_span().get_span_context()
    logger.info(json.dumps({
        "service": "eta-service",
        "message": "http request completed",
        "method": request.method,
        "path": request.url.path,
        "status": response.status_code,
        "duration_ms": round(duration * 1000, 3),
        "correlation_id": request.state.correlation_id,
        "trace_id": format(span_context.trace_id, "032x") if span_context.is_valid else "",
        "span_id": format(span_context.span_id, "016x") if span_context.is_valid else "",
    }))
    return response


@app.exception_handler(RequestValidationError)
async def validation_error(request: Request, error: RequestValidationError):
    return JSONResponse(status_code=422, content={"error": {
        "code": "validation_failed",
        "message": "request validation failed",
        "details": error.errors(include_url=False, include_input=False),
        "correlation_id": getattr(request.state, "correlation_id", ""),
    }})


@app.get("/healthz")
def healthz():
    return {"status": "up", "service": "eta-service"}


@app.get("/readyz")
def readyz():
    return {"status": "ready", "model_version": MODEL_VERSION}


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/predict-eta", response_model=ETAResponse)
def predict_eta(features: ETARequest):
    started = time.perf_counter()
    status = "error"
    try:
        result = predict(**features.model_dump())
        status = "success"
    finally:
        ETA_DURATION.labels("eta-service", status).observe(time.perf_counter() - started)
    return ETAResponse(
        predicted_eta_seconds=result.seconds,
        model_version=MODEL_VERSION,
        computed_at=datetime.now(timezone.utc),
        breakdown=Breakdown(
            travel_seconds=result.travel_seconds,
            handling_seconds=result.handling_seconds,
            load_penalty_seconds=result.load_penalty_seconds,
            availability_penalty_seconds=result.availability_penalty_seconds,
        ),
    )
