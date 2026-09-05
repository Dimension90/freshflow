package id

import "testing"

func TestNewUUID(t *testing.T) {
	value, err := NewUUID()
	if err != nil {
		t.Fatal(err)
	}
	if !IsUUID(value) {
		t.Fatalf("generated value is not a UUID: %q", value)
	}
	if value[14] != '4' {
		t.Fatalf("generated UUID version = %c, want 4", value[14])
	}
}
