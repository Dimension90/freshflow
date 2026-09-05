package catalog

import "testing"

func TestValidateReserve(t *testing.T) {
	valid := ReserveRequest{
		CheckoutAttemptID: "20000000-0000-4000-8000-000000000001",
		Items:             []ReservationItem{{ProductID: "10000000-0000-4000-8000-000000000001", Quantity: 2}},
		TTLSeconds:        600,
	}
	if err := validateReserve(valid); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
	valid.Items = append(valid.Items, valid.Items[0])
	if err := validateReserve(valid); err == nil || err.Code != "duplicate_product" {
		t.Fatalf("duplicate product error = %#v", err)
	}
}
