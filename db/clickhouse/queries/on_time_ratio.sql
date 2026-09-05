SELECT countIf(actual_eta_seconds <= predicted_eta_seconds) / nullIf(countIf(predicted_eta_seconds IS NOT NULL AND actual_eta_seconds IS NOT NULL), 0) AS on_time_ratio
FROM order_analytics FINAL
WHERE event_type = 'delivery.completed';
