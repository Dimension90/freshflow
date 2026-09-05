SELECT avg(abs(toInt64(predicted_eta_seconds) - toInt64(actual_eta_seconds))) AS mae_seconds,
       sqrt(avg(pow(toInt64(predicted_eta_seconds) - toInt64(actual_eta_seconds), 2))) AS rmse_seconds,
       avg(toInt64(predicted_eta_seconds) - toInt64(actual_eta_seconds)) AS bias_seconds
FROM order_analytics FINAL
WHERE event_type = 'delivery.completed'
  AND predicted_eta_seconds IS NOT NULL
  AND actual_eta_seconds IS NOT NULL;
