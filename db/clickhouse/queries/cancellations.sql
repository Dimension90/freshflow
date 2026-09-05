SELECT countDistinct(order_id) AS cancellations
FROM order_analytics FINAL
WHERE status = 'cancelled';
