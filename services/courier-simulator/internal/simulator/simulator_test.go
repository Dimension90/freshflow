package simulator

import (
	"testing"
	"time"
)

func TestSimulatedLocationPhases(t *testing.T) {
	now := time.Now().UTC()
	base := couriers[0]
	active := assignment{ID: "delivery", AssignedAt: now, PickupLatitude: 1, PickupLongitude: 2, DestinationLatitude: 3, DestinationLongitude: 4}
	if phase := simulatedLocation(base, active, now.Add(time.Second), 0).Phase; phase != "assembling" {
		t.Fatalf("phase = %s", phase)
	}
	if phase := simulatedLocation(base, active, now.Add(10*time.Second), 0).Phase; phase != "delivering" {
		t.Fatalf("phase = %s", phase)
	}
	if phase := simulatedLocation(base, active, now.Add(16*time.Second), 0).Phase; phase != "completed" {
		t.Fatalf("phase = %s", phase)
	}
}
