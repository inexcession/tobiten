package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	// ErrEmptySettingKey is returned when a setting key is blank.
	ErrEmptySettingKey = errors.New("setting key cannot be empty")
	// ErrEmptySettingVal is returned when a setting value is empty.
	ErrEmptySettingVal = errors.New("setting value cannot be empty")
)

// SystemSetting represents a persistent runtime setting stored in system_settings (ADR-0014 §2).
type SystemSetting struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value"` // raw JSONB bytes
	IsSecret  bool      `json:"is_secret"`
	UpdatedAt time.Time `json:"updated_at"`
	UpdatedBy *string   `json:"updated_by,omitempty"`
}

// Validate checks SystemSetting domain invariants.
func (s *SystemSetting) Validate() error {
	if strings.TrimSpace(s.Key) == "" {
		return ErrEmptySettingKey
	}
	if len(bytes.TrimSpace(s.Value)) == 0 {
		return ErrEmptySettingVal
	}
	return nil
}

// ValueString extracts a string representation of the JSONB value.
// If the value is a JSON string, it unquotes/unmarshals it.
// If the value is a JSON array of strings, it joins them with commas.
// Otherwise, it returns the trimmed string form of the raw bytes.
func (s *SystemSetting) ValueString() string {
	raw := bytes.TrimSpace(s.Value)
	if len(raw) == 0 {
		return ""
	}

	// Try unmarshaling as string
	var strVal string
	if err := json.Unmarshal(raw, &strVal); err == nil {
		return strVal
	}

	// Try unmarshaling as array of strings
	var arrVal []string
	if err := json.Unmarshal(raw, &arrVal); err == nil {
		return strings.Join(arrVal, ",")
	}

	// Return raw string (numbers, booleans, objects)
	return string(raw)
}
