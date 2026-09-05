package order

import "testing"

func TestValidTransition(t *testing.T) {
	valid := [][2]string{{"created", "confirmed"}, {"confirmed", "assembling"}, {"assembling", "delivering"}, {"delivering", "delivered"}}
	for _, transition := range valid {
		if !validTransition(transition[0], transition[1]) {
			t.Fatalf("transition %s -> %s rejected", transition[0], transition[1])
		}
	}
	if validTransition("created", "delivered") {
		t.Fatal("invalid transition was accepted")
	}
}
