# ETA service

Отдельная FastAPI inference boundary с детерминированной моделью `baseline-v1`. Входные признаки: оставшееся расстояние, число товарных позиций, стадия заказа, коэффициент загрузки района и число доступных курьеров.

```bash
curl -X POST http://localhost:8090/predict-eta \
  -H 'Content-Type: application/json' \
  -d '{"distance_km":5.2,"item_count":4,"stage":"assembling","district_load":1.2,"available_couriers":2}'
```

## Будущее обучение

`delivery.eta_predictions` сохраняет каждый feature vector, версию модели, прогноз и фактическое оставшееся время до завершения. Для обучаемой версии данные нужно выгружать временными срезами, исключать незавершённые заказы и делить train/test по времени, чтобы не допустить leakage.

Основные offline-метрики:

- MAE — понятная средняя абсолютная ошибка в секундах;
- RMSE — сильнее штрафует редкие крупные промахи;
- bias (`mean(predicted - actual)`) — показывает систематическое опоздание или завышение;
- on-time coverage — доля доставок, где `actual_eta <= predicted_eta`.

Следующая модель может быть gradient boosting по тем же стабильным HTTP-полям. Версия модели сохраняется рядом с prediction, поэтому baseline и обученную модель можно сравнивать без изменения delivery-service.
