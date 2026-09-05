WITH transitions AS
(
    SELECT order_id,
           previous_status AS status,
           dateDiff('second',
               lagInFrame(occurred_at, 1, occurred_at) OVER (
                   PARTITION BY order_id ORDER BY occurred_at
                   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
               ),
               occurred_at
           ) AS duration_seconds
    FROM order_analytics FINAL
    WHERE event_type IN ('order.created', 'order.status_changed')
)
SELECT status, avg(duration_seconds) AS average_duration_seconds
FROM transitions
WHERE status != '' AND duration_seconds > 0
GROUP BY status
ORDER BY status;
