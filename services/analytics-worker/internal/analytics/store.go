package analytics

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

type Store struct{ clickhouse driver.Conn }

func NewStore(clickhouse driver.Conn) *Store { return &Store{clickhouse: clickhouse} }

// nullable converts optional values to a nil interface or a concrete value.
// Passing a typed nil *uuid.UUID through interface{} makes clickhouse-go call
// UUID.Value() on the nil receiver and panic instead of writing SQL NULL.
func nullable[T any](value *T) any {
	if value == nil {
		return nil
	}
	return *value
}

func (s *Store) Insert(ctx context.Context, projection Projection) error {
	if projection.Delivery != nil {
		fact := projection.Delivery
		if err := s.clickhouse.Exec(ctx, `
			INSERT INTO delivery_events
			(event_id, event_type, order_id, delivery_id, courier_id, status, latitude, longitude,
			 predicted_eta_seconds, actual_eta_seconds, occurred_at, correlation_id, trace_id, payload_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			uuid.MustParse(fact.EventID), fact.EventType, nullable(fact.OrderID), nullable(fact.DeliveryID), nullable(fact.CourierID),
			fact.Status, nullable(fact.Latitude), nullable(fact.Longitude), nullable(fact.PredictedETASeconds), nullable(fact.ActualETASeconds),
			fact.OccurredAt, fact.CorrelationID, fact.TraceID, string(fact.Payload)); err != nil {
			return fmt.Errorf("insert delivery event: %w", err)
		}
	}
	if projection.Order != nil {
		fact := projection.Order
		if err := s.clickhouse.Exec(ctx, `
			INSERT INTO order_analytics
			(event_id, order_id, event_type, status, previous_status, total_amount_minor, currency,
			 product_ids, product_names, item_quantities, predicted_eta_seconds, actual_eta_seconds,
			 occurred_at, correlation_id, trace_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			uuid.MustParse(fact.EventID), uuid.MustParse(fact.OrderID), fact.EventType, fact.Status,
			fact.PreviousStatus, fact.TotalAmountMinor, fact.Currency, fact.ProductIDs, fact.ProductNames,
			fact.ItemQuantities, nullable(fact.PredictedETASeconds), nullable(fact.ActualETASeconds), fact.OccurredAt,
			fact.CorrelationID, fact.TraceID); err != nil {
			return fmt.Errorf("insert order analytic: %w", err)
		}
	}
	return nil
}

func (s *Store) OrdersByHour(ctx context.Context) ([]HourBucket, error) {
	rows, err := s.clickhouse.Query(ctx, `
		SELECT toStartOfHour(occurred_at) AS hour, count() AS orders
		FROM order_analytics FINAL
		WHERE event_type = 'order.created'
		GROUP BY hour ORDER BY hour`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]HourBucket, 0)
	for rows.Next() {
		var item HourBucket
		if err := rows.Scan(&item.Hour, &item.Orders); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) ETA(ctx context.Context) (ETAStats, error) {
	var result ETAStats
	err := s.clickhouse.QueryRow(ctx, `
		SELECT avg(predicted_eta_seconds), avg(actual_eta_seconds)
		FROM order_analytics FINAL WHERE event_type = 'delivery.completed'`).Scan(
		&result.AveragePredictedETASeconds, &result.AverageActualETASeconds)
	return result, err
}

func (s *Store) OnTimeRatio(ctx context.Context) (*float64, error) {
	var result *float64
	err := s.clickhouse.QueryRow(ctx, `
		SELECT countIf(actual_eta_seconds <= predicted_eta_seconds) /
			nullIf(countIf(predicted_eta_seconds IS NOT NULL AND actual_eta_seconds IS NOT NULL), 0)
		FROM order_analytics FINAL WHERE event_type = 'delivery.completed'`).Scan(&result)
	return result, err
}

// DeliverySLO returns a 24-hour on-time ratio and its sample size. A missing
// ratio means there were no completed deliveries in the window, not an error.
func (s *Store) DeliverySLO(ctx context.Context) (*float64, uint64, error) {
	var completed, onTime uint64
	err := s.clickhouse.QueryRow(ctx, `
		SELECT
			countIf(event_type = 'delivery.completed' AND predicted_eta_seconds IS NOT NULL AND actual_eta_seconds IS NOT NULL),
			countIf(event_type = 'delivery.completed' AND predicted_eta_seconds IS NOT NULL AND actual_eta_seconds IS NOT NULL AND actual_eta_seconds <= predicted_eta_seconds)
		FROM order_analytics FINAL
		WHERE occurred_at >= now() - INTERVAL 24 HOUR`).Scan(&completed, &onTime)
	if err != nil {
		return nil, 0, err
	}
	if completed == 0 {
		return nil, 0, nil
	}
	ratio := float64(onTime) / float64(completed)
	return &ratio, completed, nil
}

func (s *Store) Cancellations(ctx context.Context) (uint64, error) {
	var result uint64
	err := s.clickhouse.QueryRow(ctx, `
		SELECT countDistinct(order_id) FROM order_analytics FINAL WHERE status = 'cancelled'`).Scan(&result)
	return result, err
}

func (s *Store) PopularProducts(ctx context.Context) ([]ProductStat, error) {
	rows, err := s.clickhouse.Query(ctx, `
		SELECT toString(tupleElement(item, 1)) AS product_id,
		       tupleElement(item, 2) AS product_name,
		       sum(tupleElement(item, 3)) AS quantity
		FROM (
			SELECT arrayJoin(arrayZip(product_ids, product_names, item_quantities)) AS item
			FROM order_analytics FINAL WHERE event_type = 'order.created'
		)
		GROUP BY product_id, product_name ORDER BY quantity DESC, product_name LIMIT 10`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]ProductStat, 0)
	for rows.Next() {
		var item ProductStat
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) StatusDurations(ctx context.Context) ([]StatusDuration, error) {
	rows, err := s.clickhouse.Query(ctx, `
		WITH transitions AS (
			SELECT order_id, previous_status AS status,
				dateDiff('second', lagInFrame(occurred_at, 1, occurred_at) OVER (
					PARTITION BY order_id ORDER BY occurred_at
					ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), occurred_at) AS duration_seconds
			FROM order_analytics FINAL WHERE event_type IN ('order.created', 'order.status_changed')
		)
		SELECT status, avg(duration_seconds) FROM transitions
		WHERE status != '' AND duration_seconds > 0 GROUP BY status ORDER BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]StatusDuration, 0)
	for rows.Next() {
		var item StatusDuration
		if err := rows.Scan(&item.Status, &item.AverageDurationSeconds); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) Summary(ctx context.Context) (Summary, error) {
	var result Summary
	var err error
	if result.OrdersByHour, err = s.OrdersByHour(ctx); err != nil {
		return result, err
	}
	if result.ETA, err = s.ETA(ctx); err != nil {
		return result, err
	}
	if result.OnTimeRatio, err = s.OnTimeRatio(ctx); err != nil {
		return result, err
	}
	if result.Cancellations, err = s.Cancellations(ctx); err != nil {
		return result, err
	}
	if result.PopularProducts, err = s.PopularProducts(ctx); err != nil {
		return result, err
	}
	if result.StatusDurations, err = s.StatusDurations(ctx); err != nil {
		return result, err
	}
	return result, nil
}
