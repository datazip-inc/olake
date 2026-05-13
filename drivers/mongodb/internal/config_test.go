package driver

import (
	"strings"
	"testing"
)

func TestConfig_URI(t *testing.T) {
	tests := []struct {
		name             string
		config           *Config
		expectedContains []string
		notExpected      []string
	}{
		{
			// Leaves read preference unset for standalone non-SRV connections.
			name: "without replica set or srv",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Username: "mongodb",
				Password: "secret",
				AuthDB:   "admin",
			},
			expectedContains: []string{
				"mongodb://mongodb:secret@mongo.internal:27017/",
				"authSource=admin",
			},
			notExpected: []string{
				"readPreference=",
				"replicaSet=",
			},
		},
		{
			// Preserves the default read preference for replica set connections.
			name: "with replica set and default read preference",
			config: &Config{
				Hosts:      []string{"mongo-1.internal:27017", "mongo-2.internal:27017"},
				Username:   "mongodb",
				Password:   "secret",
				AuthDB:     "admin",
				ReplicaSet: "rs0",
			},
			expectedContains: []string{
				"replicaSet=rs0",
				"readPreference=secondaryPreferred",
			},
		},
		{
			// Applies the default read preference for SRV-based connections.
			name: "with srv and default read preference",
			config: &Config{
				Hosts:    []string{"cluster0.example.mongodb.net"},
				Username: "mongodb",
				Password: "secret",
				AuthDB:   "admin",
				Srv:      true,
			},
			expectedContains: []string{
				"mongodb+srv://mongodb:secret@cluster0.example.mongodb.net/",
				"readPreference=secondaryPreferred",
			},
			notExpected: []string{
				"replicaSet=",
			},
		},
		{
			// Honors an explicit read preference without requiring a replica set name.
			name: "with explicit read preference and no replica set",
			config: &Config{
				Hosts:          []string{"mongo.internal:27017"},
				Username:       "mongodb",
				Password:       "secret",
				AuthDB:         "admin",
				ReadPreference: "nearest",
			},
			expectedContains: []string{
				"readPreference=nearest",
			},
			notExpected: []string{
				"replicaSet=",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uri := tt.config.URI()

			for _, expected := range tt.expectedContains {
				if !strings.Contains(uri, expected) {
					t.Errorf("expected URI to contain %q, got %s", expected, uri)
				}
			}

			for _, notExpected := range tt.notExpected {
				if strings.Contains(uri, notExpected) {
					t.Errorf("expected URI to not contain %q, got %s", notExpected, uri)
				}
			}
		})
	}
}
