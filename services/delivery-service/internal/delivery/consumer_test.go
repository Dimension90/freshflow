package delivery

import "testing"

func TestValidDeliveryTransition(t *testing.T) {
	if !validDeliveryTransition("assigned", "assembling") || !validDeliveryTransition("assembling", "delivering") || !validDeliveryTransition("delivering", "completed") {
		t.Fatal("valid delivery transition rejected")
	}
	if validDeliveryTransition("assigned", "completed") {
		t.Fatal("invalid delivery transition accepted")
	}
}
