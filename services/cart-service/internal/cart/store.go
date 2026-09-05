package cart

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type Store struct {
	postgres *pgxpool.Pool
	redis    *redis.Client
	cacheTTL time.Duration
}

func NewStore(postgres *pgxpool.Pool, redisClient *redis.Client, cacheTTL time.Duration) *Store {
	return &Store{postgres: postgres, redis: redisClient, cacheTTL: cacheTTL}
}

func (s *Store) Get(ctx context.Context, userID string) (Cart, error) {
	key := cartCacheKey(userID)
	if encoded, err := s.redis.Get(ctx, key).Bytes(); err == nil {
		var cached Cart
		if json.Unmarshal(encoded, &cached) == nil {
			return cached, nil
		}
	}
	value, err := getCart(ctx, s.postgres, userID)
	if err != nil {
		return Cart{}, err
	}
	if encoded, err := json.Marshal(value); err == nil {
		_ = s.redis.Set(ctx, key, encoded, s.cacheTTL).Err()
	}
	return value, nil
}

func (s *Store) SetItem(ctx context.Context, userID, productID string, quantity int) (Cart, error) {
	tx, err := s.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Cart{}, fmt.Errorf("begin cart transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO cart.carts (user_id) VALUES ($1)
		ON CONFLICT (user_id) DO UPDATE SET version = cart.carts.version + 1, updated_at = now()`, userID); err != nil {
		return Cart{}, fmt.Errorf("upsert cart: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO cart.cart_items (user_id, product_id, quantity) VALUES ($1, $2, $3)
		ON CONFLICT (user_id, product_id) DO UPDATE SET quantity = EXCLUDED.quantity, updated_at = now()`, userID, productID, quantity); err != nil {
		return Cart{}, fmt.Errorf("upsert cart item: %w", err)
	}
	value, err := getCart(ctx, tx, userID)
	if err != nil {
		return Cart{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Cart{}, fmt.Errorf("commit cart: %w", err)
	}
	_ = s.redis.Del(ctx, cartCacheKey(userID)).Err()
	return value, nil
}

func (s *Store) DeleteItem(ctx context.Context, userID, productID string) (Cart, error) {
	tx, err := s.postgres.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Cart{}, fmt.Errorf("begin cart transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	command, err := tx.Exec(ctx, `DELETE FROM cart.cart_items WHERE user_id = $1 AND product_id = $2`, userID, productID)
	if err != nil {
		return Cart{}, fmt.Errorf("delete cart item: %w", err)
	}
	if command.RowsAffected() > 0 {
		if _, err := tx.Exec(ctx, `UPDATE cart.carts SET version = version + 1, updated_at = now() WHERE user_id = $1`, userID); err != nil {
			return Cart{}, fmt.Errorf("update cart version: %w", err)
		}
	}
	value, err := getCart(ctx, tx, userID)
	if err != nil {
		return Cart{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Cart{}, fmt.Errorf("commit cart: %w", err)
	}
	_ = s.redis.Del(ctx, cartCacheKey(userID)).Err()
	return value, nil
}

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func getCart(ctx context.Context, db querier, userID string) (Cart, error) {
	value := Cart{UserID: userID, Items: make([]Item, 0)}
	err := db.QueryRow(ctx, `SELECT version, updated_at FROM cart.carts WHERE user_id = $1`, userID).Scan(&value.Version, &value.UpdatedAt)
	if err == pgx.ErrNoRows {
		return value, nil
	}
	if err != nil {
		return Cart{}, fmt.Errorf("query cart: %w", err)
	}
	rows, err := db.Query(ctx, `SELECT product_id::text, quantity FROM cart.cart_items WHERE user_id = $1 ORDER BY product_id`, userID)
	if err != nil {
		return Cart{}, fmt.Errorf("query cart items: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Item
		if err := rows.Scan(&item.ProductID, &item.Quantity); err != nil {
			return Cart{}, fmt.Errorf("scan cart item: %w", err)
		}
		value.Items = append(value.Items, item)
	}
	return value, rows.Err()
}

func cartCacheKey(userID string) string { return "local:cart:" + userID }
