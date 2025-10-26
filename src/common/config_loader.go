package common

import (
	"encoding/json"
	"fmt"
	"os"
)

// LoadConfig loads the system configuration from the given JSON file.
// It falls back to DefaultConfig when the file does not exist or cannot be parsed.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()

	file, err := os.Open(path)
	if err != nil {
		return cfg, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	cfg.ApplyDefaults()
	return cfg, nil
}
