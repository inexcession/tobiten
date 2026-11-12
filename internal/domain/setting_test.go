package domain

import (
	"errors"
	"testing"
	"time"
)

func TestSystemSetting_Validate(t *testing.T) {
	tests := []struct {
		name    string
		setting SystemSetting
		wantErr error
	}{
		{
			name: "valid setting",
			setting: SystemSetting{
				Key:       "oidc.default_role_id",
				Value:     []byte(`"019318a0-0000-7000-8000-000000000003"`),
				UpdatedAt: time.Now(),
			},
			wantErr: nil,
		},
		{
			name: "empty key",
			setting: SystemSetting{
				Key:   "",
				Value: []byte(`"value"`),
			},
			wantErr: ErrEmptySettingKey,
		},
		{
			name: "whitespace-only key",
			setting: SystemSetting{
				Key:   "   ",
				Value: []byte(`"value"`),
			},
			wantErr: ErrEmptySettingKey,
		},
		{
			name: "empty value",
			setting: SystemSetting{
				Key:   "server.port",
				Value: nil,
			},
			wantErr: ErrEmptySettingVal,
		},
		{
			name: "whitespace-only value",
			setting: SystemSetting{
				Key:   "server.port",
				Value: []byte("   "),
			},
			wantErr: ErrEmptySettingVal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setting.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSystemSetting_ValueString(t *testing.T) {
	tests := []struct {
		name    string
		value   []byte
		wantVal string
	}{
		{
			name:    "empty value",
			value:   nil,
			wantVal: "",
		},
		{
			name:    "json string",
			value:   []byte(`"info"`),
			wantVal: "info",
		},
		{
			name:    "json number",
			value:   []byte(`8080`),
			wantVal: "8080",
		},
		{
			name:    "json boolean",
			value:   []byte(`true`),
			wantVal: "true",
		},
		{
			name:    "json string array",
			value:   []byte(`["server-label-1", "server-label-2"]`),
			wantVal: "server-label-1,server-label-2",
		},
		{
			name:    "plain unquoted string",
			value:   []byte(`warn`),
			wantVal: "warn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SystemSetting{
				Key:   "test.key",
				Value: tt.value,
			}
			if got := s.ValueString(); got != tt.wantVal {
				t.Errorf("ValueString() = %q, want %q", got, tt.wantVal)
			}
		})
	}
}
