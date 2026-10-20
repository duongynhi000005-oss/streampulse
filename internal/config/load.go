// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 StreamPulse contributors

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Load builds Config from defaults, optional YAML file, STREAMPULSE_* env
// vars, and flag overrides (in that precedence order).
func Load(flags *pflag.FlagSet) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("STREAMPULSE")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	cfg := DefaultConfig()
	setViperDefaults(v, reflect.ValueOf(cfg), "")

	if path := configPath(flags); path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
	}

	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if flags != nil {
		if f := flags.Lookup("brokers"); f != nil {
			if b, err := flags.GetStringSlice("brokers"); err == nil && len(b) > 0 {
				cfg.Brokers = b
			}
		}
	}
	if _, err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// loadFromFile loads Config from a specific YAML file on top of defaults.
func loadFromFile(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	cfg := DefaultConfig()
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	if _, err := Validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// setViperDefaults registers every Config leaf key with its default value so
// that AutomaticEnv lookups and Unmarshal see them (viper only enumerates
// keys it knows about).
func setViperDefaults(v *viper.Viper, val reflect.Value, prefix string) {
	switch val.Kind() {
	case reflect.Pointer:
		if !val.IsNil() {
			setViperDefaults(v, val.Elem(), prefix)
		}
	case reflect.Struct:
		t := val.Type()
		for i := 0; i < t.NumField(); i++ {
			sf := t.Field(i)
			tag := strings.Split(sf.Tag.Get("mapstructure"), ",")[0]
			if tag == "" {
				continue
			}
			key := tag
			if prefix != "" {
				key = prefix + "." + tag
			}
			setViperDefaults(v, val.Field(i), key)
		}
	case reflect.Slice:
		if val.IsNil() {
			v.SetDefault(prefix, []any{})
		} else {
			v.SetDefault(prefix, val.Interface())
		}
	default:
		v.SetDefault(prefix, val.Interface())
	}
}

func configPath(flags *pflag.FlagSet) string {
	if flags != nil {
		if p, err := flags.GetString("config"); err == nil && p != "" {
			return p
		}
	}
	if p := os.Getenv("STREAMPULSE_CONFIG"); p != "" {
		return p
	}
	for _, p := range []string{
		filepath.Join(os.Getenv("HOME"), ".config", "streampulse", "streampulse.yaml"),
		"/etc/streampulse/streampulse.yaml",
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// ParseScrapeInterval parses and bounds the scrape interval. Named
// ParseScrapeInterval because the Config field ScrapeInterval prevents a
// same-named method (deviation from the plan's ScrapeInterval method).
func (c *Config) ParseScrapeInterval() (time.Duration, error) {
	d, err := time.ParseDuration(c.ScrapeInterval)
	if err != nil {
		return 0, fmt.Errorf("scrape_interval %q: %w", c.ScrapeInterval, err)
	}
	if d < time.Second || d > time.Minute {
		return 0, fmt.Errorf("scrape_interval must be between 1s and 1m, got %s", d)
	}
	return d, nil
}
