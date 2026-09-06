package main

import "testing"

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  config
		wantErr bool
	}{
		{
			name:   "dry run is safe by default",
			config: config{topic: "freshflow.order.events.v1.dlq", limit: 1, timeout: 1},
		},
		{
			name:    "replay requires confirmation",
			config:  config{topic: "freshflow.order.events.v1.dlq", limit: 1, timeout: 1, execute: true},
			wantErr: true,
		},
		{
			name:   "confirmed replay",
			config: config{topic: "freshflow.order.events.v1.dlq", limit: 1, timeout: 1, execute: true, confirm: confirmationPhrase},
		},
		{
			name:    "source topic is rejected",
			config:  config{topic: "freshflow.order.events.v1", limit: 1, timeout: 1},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.config.validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("validate() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}
