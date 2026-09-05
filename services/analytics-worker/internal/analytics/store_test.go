package analytics

import (
	"testing"

	"github.com/google/uuid"
)

func TestNullableReturnsNilInterfaceForNilUUID(t *testing.T) {
	if got := nullable[uuid.UUID](nil); got != nil {
		t.Fatalf("nullable(nil) = %#v, want nil interface", got)
	}
}

func TestNullableDereferencesValue(t *testing.T) {
	expected := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	got, ok := nullable(&expected).(uuid.UUID)
	if !ok || got != expected {
		t.Fatalf("nullable() = %#v, want %s", nullable(&expected), expected)
	}
}
