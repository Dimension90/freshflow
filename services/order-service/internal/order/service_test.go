package order

import "testing"

func TestRequestHashIsStableAndPayloadSpecific(t *testing.T) {
	first := CheckoutRequest{UserID: "00000000-0000-4000-8000-000000000001", CartVersion: 3}
	if requestHash(first) != requestHash(first) {
		t.Fatal("request hash is not stable")
	}
	second := first
	second.CartVersion++
	if requestHash(first) == requestHash(second) {
		t.Fatal("request hash does not include cart version")
	}
}

func TestCanCancelOnlyBeforeAssembly(t *testing.T) {
	for _, status := range []string{"created", "confirmed"} {
		if !canCancel(status) {
			t.Fatalf("expected %q to be cancellable", status)
		}
	}
	for _, status := range []string{"assembling", "delivering", "delivered", "cancelled"} {
		if canCancel(status) {
			t.Fatalf("expected %q to be terminal or in progress", status)
		}
	}
}
