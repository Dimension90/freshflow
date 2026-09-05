SELECT tupleElement(item, 1) AS product_id,
       tupleElement(item, 2) AS product_name,
       sum(tupleElement(item, 3)) AS quantity
FROM
(
    SELECT arrayJoin(arrayZip(product_ids, product_names, item_quantities)) AS item
    FROM order_analytics FINAL
    WHERE event_type = 'order.created'
)
GROUP BY product_id, product_name
ORDER BY quantity DESC, product_name
LIMIT 10;
