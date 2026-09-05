package envconfig

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func String(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func Int(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return value, nil
}

func Duration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration", key)
	}
	return value, nil
}

func CSV(key, fallback string) []string {
	parts := strings.Split(String(key, fallback), ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}
