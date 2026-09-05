SELECT avg(predicted_eta_seconds) AS average_predicted_eta_seconds,
       avg(actual_eta_seconds) AS average_actual_eta_seconds
FROM order_analytics FINAL
WHERE event_type = 'delivery.completed';
