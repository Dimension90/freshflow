SELECT toStartOfHour(occurred_at) AS hour, count() AS orders
FROM order_analytics FINAL
WHERE event_type = 'order.created'
GROUP BY hour
ORDER BY hour;
