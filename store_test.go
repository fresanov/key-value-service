package main

import (
	"testing"
)

func TestStore(t *testing.T) {
	tests := []struct {
		name      string
		operation func() error
		key       string
		value     string
		wantErr   error
	}{
		{
			name: "put value in store and check that it's there",
			operation: func() error {
				return Put("key1", "value1")
			},
			key:     "key1",
			value:   "value1",
			wantErr: nil,
		},
		{
			name: "put same value twice and make sure it's put only once",
			operation: func() error {
				Put("key2", "value2")
				return Put("key2", "value2")
			},
			key:     "key2",
			value:   "value2",
			wantErr: nil,
		},
		{
			name: "get value after putting it returns the value and no error",
			operation: func() error {
				Put("key3", "value3")
				_, err := Get("key3")
				return err
			},
			key:     "key3",
			value:   "value3",
			wantErr: nil,
		},
		{
			name: "getting non existing value returns no such key error",
			operation: func() error {
				_, err := Get("nonexistent")
				return err
			},
			key:     "nonexistent",
			value:   "",
			wantErr: ErrNoSuchKey,
		},
		{
			name: "getting value after deleting it returns no such key",
			operation: func() error {
				Put("key4", "value4")
				Delete("key4")
				_, err := Get("key4")
				return err
			},
			key:     "key4",
			value:   "",
			wantErr: ErrNoSuchKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			if err != tt.wantErr {
				t.Errorf("got error %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil {
				value, err := Get(tt.key)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if value != tt.value {
					t.Errorf("got value %v, want %v", value, tt.value)
				}
			}
		})
	}
}

func TestDeleteNonExistingValue(t *testing.T) {
	err := Delete("nonexistent")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}
