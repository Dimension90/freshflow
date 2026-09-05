package notification

import "testing"

func TestMessageFor(t *testing.T) {
	if message, ok := messageFor("order.created"); !ok || message == "" {
		t.Fatalf("order.created message = %q, supported=%v", message, ok)
	}
	if _, ok := messageFor("courier.location_updated"); ok {
		t.Fatal("unsupported event was accepted")
	}
}
